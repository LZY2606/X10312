package formstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"sync"

	conditionjudge "github.com/mazrean/formstream/internal/condition_judge"
)

var (
	// ErrTooManyParts is returned when the parts are more than MaxParts.
	ErrTooManyParts = errors.New("too many parts")
	// ErrTooManyHeaders is returned when the headers are more than MaxHeaders.
	ErrTooManyHeaders = errors.New("too many headers")
	// ErrTooLargeForm is returned when the form is too large for the parser to handle within the memory limit.
	ErrTooLargeForm = errors.New("too large form")
)

// Parse parses the multipart form from r.
func (p *Parser) Parse(r io.Reader) (err error) {
	return p.ParseWithContext(context.Background(), r)
}

// ParseWithContext parses the multipart form from r, honoring ctx for
// cooperative cancellation.
//
// Parsing always runs on the same code path as Parse. When ctx is canceled
// before parsing finishes, ParseWithContext interrupts the in-flight Read by
// closing r when it implements io.Closer (as multipart request bodies do),
// and then waits for the parsing goroutine to return. ParseWithContext never
// returns while the producer goroutine is still running.
//
// The first causal error is preserved: a normal parse failure is returned as
// is, and cancellation only replaces it when parsing had not already failed
// before ctx was canceled.
func (p *Parser) ParseWithContext(ctx context.Context, r io.Reader) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	hsc := newHookSatisfactionChecker(p.hookMap, &p.parserConfig)
	defer func() {
		deferErr := hsc.Close()
		// capture the error of Close()
		if deferErr != nil {
			if err != nil {
				err = errors.Join(err, deferErr)
			} else {
				err = deferErr
			}
		}
	}()

	done := make(chan struct{})
	cr := &contextReader{r: r}
	go func() {
		defer close(done)
		err = p.parse(cr, hsc.IConditionJudger)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// Interrupt a Read blocked on r, then wait for the producer goroutine
		// so ParseWithContext never returns while parsing is still running.
		cr.cancel(ctx.Err())
		<-done
	}

	return
}

// contextReader wraps the parser input. Read behaves exactly like the wrapped
// reader until cancel is called; after cancel it short-circuits with the
// cancellation cause. cancel also closes the wrapped reader when it implements
// io.Closer, which unblocks a Read parked inside the underlying transport.
type contextReader struct {
	r      io.Reader
	cancel chan error
}

func (cr *contextReader) Read(p []byte) (int, error) {
	select {
	case err := <-cr.cancel:
		return 0, err
	default:
	}

	if cr.cancel == nil {
		return cr.r.Read(p)
	}

	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		n, err := cr.r.Read(p)
		ch <- result{n, err}
	}()

	select {
	case err := <-cr.cancel:
		if c, ok := cr.r.(io.Closer); ok {
			_ = c.Close()
		}
		<-ch
		return 0, err
	case res := <-ch:
		return res.n, res.err
	}
}

func (cr *contextReader) cancel(err error) {
	cr.cancel = make(chan error, 1)
	cr.cancel <- err
}

func (p *Parser) parse(r io.Reader, hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]) error {
	mr := multipart.NewReader(r, p.boundary)
	for {
		var part *multipart.Part
		part, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next part: %w", err)
		}

		if p.maxParts == 0 {
			return ErrTooManyParts
		}
		p.maxParts--

		for _, header := range part.Header {
			if p.maxHeaders < uint(len(header)) {
				return ErrTooManyHeaders
			}
			p.maxHeaders -= uint(len(header))
		}

		header := newHeader(part.Header)
		if hsc.IsHookExist(part.FormName()) {
			_, err := hsc.HookEvent(part.FormName(), &normalParam{
				r: part,
				h: header,
			})
			if err != nil {
				return fmt.Errorf("failed to run or set hook: %w", err)
			}
		} else {
			b := new(bytes.Buffer)

			if p.maxMemSize <= DataSize(len(part.FormName())) {
				return ErrTooLargeForm
			}
			p.maxMemSize -= DataSize(len(part.FormName()))

			// Enforce the budget while copying instead of after io.Copy so a
			// value larger than the remaining budget is never fully buffered
			// before the error is reported.
			n, err := io.Copy(b, io.LimitReader(part, int64(p.maxMemSize)+1))
			if err != nil {
				return fmt.Errorf("failed to copy part: %w", err)
			}
			if DataSize(n) > p.maxMemSize {
				return ErrTooLargeForm
			}
			p.maxMemSize -= DataSize(n)

			p.valueMap[part.FormName()] = append(p.valueMap[part.FormName()], Value{
				content: b.Bytes(),
				header:  header,
			})
		}

		err = hsc.KeyEvent(part.FormName())
		if err != nil {
			return fmt.Errorf("failed to run satisfied hook: %w", err)
		}
	}

	return nil
}

type hookSatisfactionChecker struct {
	conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]
	preProcessor *preProcessor
}

func newHookSatisfactionChecker(streamHooks map[string]streamHook, config *parserConfig) *hookSatisfactionChecker {
	judgeHooks := make(map[string]conditionjudge.Hook[string, *normalParam, *abnormalParam], len(streamHooks))
	for name, hook := range streamHooks {
		h := judgeHook(hook)
		judgeHooks[name] = &h
	}

	preProcess := &preProcessor{
		config:  config,
		pending: make(map[*memCloserState]struct{}),
	}

	return &hookSatisfactionChecker{
		IConditionJudger: conditionjudge.NewConditionJudger(judgeHooks, preProcess.run),
		preProcessor:     preProcess,
	}
}

func (wh *hookSatisfactionChecker) Close() error {
	return wh.preProcessor.Close()
}

type normalParam struct {
	r io.Reader
	h Header
}

type abnormalParam struct {
	content io.ReadCloser
	header  Header
}

type preProcessor struct {
	config  *parserConfig
	offset  int64
	file    *os.File
	pending map[*memCloserState]struct{}
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (pp *preProcessor) run(normalParam *normalParam) (*abnormalParam, error) {
	buf, ok := bufPool.Get().(*bytes.Buffer)
	if !ok {
		buf = new(bytes.Buffer)
	}
	buf.Reset()

	memLimit := min(pp.config.maxMemFileSize, pp.config.maxMemSize)
	n, err := io.CopyN(buf, normalParam.r, int64(memLimit)+1)
	if err != nil && !errors.Is(err, io.EOF) {
		bufPool.Put(buf)
		return nil, fmt.Errorf("failed to copy: %w", err)
	}

	var content io.ReadCloser
	if DataSize(n) > memLimit {
		if pp.file == nil {
			f, err := os.CreateTemp("", "formstream-")
			if err != nil {
				bufPool.Put(buf)
				return nil, fmt.Errorf("failed to create temp file: %w", err)
			}
			pp.file = f
		}

		bufSize, err := io.Copy(pp.file, buf)
		if err != nil {
			bufPool.Put(buf)
			return nil, fmt.Errorf("failed to write: %w", err)
		}

		remainSize, err := io.Copy(pp.file, normalParam.r)
		if err != nil {
			bufPool.Put(buf)
			return nil, fmt.Errorf("failed to copy: %w", err)
		}

		size := bufSize + remainSize
		content = io.NopCloser(io.NewSectionReader(pp.file, pp.offset, size))
		pp.offset += size

		bufPool.Put(buf)
	} else {
		pp.config.maxMemSize -= DataSize(buf.Len())
		pp.config.maxMemFileSize -= DataSize(buf.Len())
		bufSize := buf.Len()
		state := &memCloserState{buf: buf, size: bufSize, config: pp.config, onClose: pp.removePending}
		pp.pending[state] = struct{}{}
		content = customReadCloser{
			Reader:     buf,
			closeFunc:  state.close,
		}
	}

	return &abnormalParam{
		content: content,
		header:  normalParam.h,
	}, nil
}

func (pp *preProcessor) Close() error {
	// Release any spooled in-memory contents whose hooks never ran (for
	// example because parsing, a consumer or the context failed first).
	for state := range pp.pending {
		_ = state.close()
	}
	pp.pending = nil

	if pp.file == nil {
		return nil
	}

	filepath := pp.file.Name()

	// Close the file handle first
	closeErr := pp.file.Close()

	// Remove the temporary file from disk
	removeErr := os.Remove(filepath)

	// Return combined errors if any
	if closeErr != nil || removeErr != nil {
		return errors.Join(closeErr, removeErr)
	}

	return nil
}

type judgeHook struct {
	fn           StreamHookFunc
	requireParts []string
}

func (jh judgeHook) NormalPath(normalParam *normalParam) error {
	return jh.fn(normalParam.r, normalParam.h)
}

func (jh judgeHook) AbnormalPath(abnoramlParam *abnormalParam) error {
	defer abnoramlParam.content.Close()

	return jh.fn(abnoramlParam.content, abnoramlParam.header)
}

func (jh judgeHook) Requirements() []string {
	return jh.requireParts
}

type customReadCloser struct {
	io.Reader
	closeFunc func() error
}

func (cc customReadCloser) Close() error {
	return cc.closeFunc()
}

// memCloserState backs the in-memory spooled content. close is idempotent so
// double closes (hook and end-of-parse cleanup) return the buffer to the pool
// and restore the budget exactly once.
type memCloserState struct {
	buf     *bytes.Buffer
	size    int
	config  *parserConfig
	onClose func(*memCloserState)
	once    sync.Once
	err     error
}

func (s *memCloserState) close() error {
	s.once.Do(func() {
		if s.onClose != nil {
			s.onClose(s)
		}
		bufPool.Put(s.buf)
		s.config.maxMemSize += DataSize(s.size)
		s.config.maxMemFileSize += DataSize(s.size)
	})
	return s.err
}

func (pp *preProcessor) removePending(state *memCloserState) {
	delete(pp.pending, state)
}
