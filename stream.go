package formstream

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

const (
	// defaultStreamBlockSize is the granularity of bytes handed from the
	// producer goroutine to the hook-running goroutine.
	defaultStreamBlockSize = 32 * 1024
	// defaultStreamMaxInflight bounds how many blocks may wait between the
	// producer and the consumer, so a slow consumer back-pressures the body
	// reader and the parser can never buffer a whole large file. 32KiB * 16 is
	// a 512KiB ceiling per live part.
	defaultStreamMaxInflight = 16
)

// streamMetrics records parser-wide streaming counters. All fields are safe
// for concurrent access; the type is unexported and read by internal tests
// that exercise the same production parse loop.
type streamMetrics struct {
	inflight atomic.Int64
	peak     atomic.Int64
}

func (m *streamMetrics) trackInflight(delta int) {
	if m == nil {
		return
	}
	cur := m.inflight.Add(int64(delta))
	if delta > 0 {
		for {
			peak := m.peak.Load()
			if cur <= peak || m.peak.CompareAndSwap(peak, cur) {
				return
			}
		}
	}
}

// firstErr records the first non-nil causal error seen from either goroutine.
type firstErr struct {
	mu  sync.Mutex
	err error
}

func (fe *firstErr) set(err error) {
	if err == nil {
		return
	}
	fe.mu.Lock()
	if fe.err == nil {
		fe.err = err
	}
	fe.mu.Unlock()
}

func (fe *firstErr) get() error {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	return fe.err
}

type hookJob struct {
	fn   func() error
	done chan error
}

// asyncExec owns the hand-off between the producer goroutine (the parse loop)
// and the caller goroutine (user hooks).
type asyncExec struct {
	ctx         context.Context
	jobs        chan hookJob
	first       firstErr
	blockSize   int
	maxInflight int
	metrics     *streamMetrics
	consumers   sync.WaitGroup
}

func newAsyncExec(ctx context.Context, c *parserConfig, metrics *streamMetrics) *asyncExec {
	blockSize := c.streamBlockSize
	if blockSize <= 0 {
		blockSize = defaultStreamBlockSize
	}
	maxInflight := c.streamMaxInflight
	if maxInflight <= 0 {
		maxInflight = defaultStreamMaxInflight
	}
	return &asyncExec{
		ctx:         ctx,
		jobs:        make(chan hookJob, 1),
		blockSize:   blockSize,
		maxInflight: maxInflight,
		metrics:     metrics,
	}
}

// run is the caller-goroutine side: every hook job is executed serially, in
// the exact order the parse loop produced it. Jobs still enqueued while the
// producer is shutting down are executed as well, so spooled content's Close
// hooks always run and pool buffers / memory budget are released.
func (ex *asyncExec) run() {
	defer ex.consumers.Done()
	for job := range ex.jobs {
		err := job.fn()
		if err != nil {
			ex.first.set(err)
		}
		job.done <- err
	}
}

// submit enqueues an already-materialized (spooled) hook invocation and waits
// for its result, preserving hook call ordering on the caller goroutine.
func (ex *asyncExec) submit(fn func() error) error {
	done := make(chan error, 1)
	select {
	case ex.jobs <- hookJob{fn: fn, done: done}:
	case <-ex.ctx.Done():
		return ex.ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-ex.ctx.Done():
		// The job keeps running on the consumer side and its result lands in
		// the buffered channel; cancellation is the caller-visible cause.
		return ex.ctx.Err()
	}
}

// dispatch streams one live part: it hands the hook a bounded pipe reader and
// pumps the part's bytes on the producer goroutine while the hook runs on the
// caller goroutine. Ordering is identical to the synchronous loop.
func (ex *asyncExec) dispatch(fn StreamHookFunc, param *normalParam) error {
	pipe := newPartPipe(ex)

	done := make(chan error, 1)
	select {
	case ex.jobs <- hookJob{
		fn: func() error {
			err := fn(pipe, param.h)
			pipe.markHookDone(err)
			return err
		},
		done: done,
	}:
	case <-ex.ctx.Done():
		return ex.ctx.Err()
	}

	pumpErr := pipe.pump(param.r)

	var hookErr error
	select {
	case hookErr = <-done:
	case <-ex.ctx.Done():
		hookErr = ex.ctx.Err()
	}

	if hookErr != nil {
		return hookErr
	}
	return pumpErr
}

// partPipe is the bounded single-producer/single-consumer byte stream between
// the parse loop and a hook.
type partPipe struct {
	ex      *asyncExec
	blocks  chan streamBlock
	abort   chan struct{}
	current []byte
	hookMu  sync.Mutex
	hookErr error
}

type streamBlock struct {
	data []byte
	err  error
}

func newPartPipe(ex *asyncExec) *partPipe {
	return &partPipe{
		ex:     ex,
		blocks: make(chan streamBlock, ex.maxInflight),
		abort:  make(chan struct{}),
	}
}

// markHookDone records the hook result and unblocks the pump. On success the
// pump drains the remainder to the part boundary; on failure or cancellation
// it stops immediately.
func (p *partPipe) markHookDone(err error) {
	p.hookMu.Lock()
	p.hookErr = err
	p.hookMu.Unlock()
	close(p.abort)
}

// Read implements io.Reader for the hook.
func (p *partPipe) Read(out []byte) (int, error) {
	if len(p.current) == 0 {
		block, ok := <-p.blocks
		if !ok {
			select {
			case <-p.ex.ctx.Done():
				return 0, p.ex.ctx.Err()
			default:
				return 0, io.ErrUnexpectedEOF
			}
		}
		if block.err != nil {
			return 0, block.err
		}
		p.current = block.data
	}

	n := copy(out, p.current)
	p.current = p.current[n:]
	return n, nil
}

// pump copies src into the pipe until EOF, a read error, hook completion, or
// context cancellation. A successful hook that stopped early still causes the
// remainder to be drained here so multipart framing stays intact.
func (p *partPipe) pump(src io.Reader) error {
	defer close(p.blocks)

	for {
		select {
		case <-p.abort:
			return p.afterAbort(src)
		default:
		}

		buf := make([]byte, p.ex.blockSize)
		n, readErr := src.Read(buf)

		if n > 0 {
			p.ex.metrics.trackInflight(n)
			select {
			case p.blocks <- streamBlock{data: buf[:n]}:
				p.ex.metrics.trackInflight(-n)
			case <-p.abort:
				return p.afterAbort(src)
			case <-p.ex.ctx.Done():
				return p.ex.ctx.Err()
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			select {
			case p.blocks <- streamBlock{err: readErr}:
			case <-p.abort:
			case <-p.ex.ctx.Done():
			}
			return readErr
		}
	}
}

// afterAbort handles the pipe once the hook has finished. On hook success the
// unread body is drained to the part boundary (this is what the synchronous
// path relies on via NextPart's internal skip). On hook error or cancellation
// the producer aborts and surfaces the causal error instead.
func (p *partPipe) afterAbort(src io.Reader) error {
	p.hookMu.Lock()
	hookErr := p.hookErr
	p.hookMu.Unlock()

	if hookErr != nil {
		return nil
	}
	select {
	case <-p.ex.ctx.Done():
		return nil
	default:
	}

	if _, err := io.Copy(io.Discard, src); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// parseAsync runs the same parse loop as parse on a producer goroutine while
// hooks are executed serially on the calling goroutine.
func (p *Parser) parseAsync(r io.Reader) (err error) {
	ctx := p.ctx
	ex := newAsyncExec(ctx, &p.parserConfig, &p.streamMetrics)
	hsc := newAsyncHookSatisfactionChecker(p.hookMap, &p.parserConfig, ex)

	defer func() {
		deferErr := hsc.Close()
		if deferErr != nil {
			if err != nil {
				err = errors.Join(err, deferErr)
			} else {
				err = deferErr
			}
		}
	}()

	ex.consumers.Add(1)
	go ex.run()

	producerDone := make(chan struct{})
	go func() {
		perr := p.parse(r, hsc.IConditionJudger)
		ex.first.set(perr)
		close(producerDone)
	}()

	select {
	case <-producerDone:
	case <-ctx.Done():
		ex.first.set(fmtErrorfCancel(ctx))
	}
	<-producerDone

	close(ex.jobs)
	ex.consumers.Wait()

	if first := ex.first.get(); first != nil {
		err = first
	}
	return
}
