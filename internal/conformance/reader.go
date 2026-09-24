package conformance

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// ReaderStats is the observable I/O footprint of one parse attempt.
type ReaderStats struct {
	ReadCalls  atomic.Int64
	CloseCalls atomic.Int64
	BytesRead  atomic.Int64
	// InFlight counts bytes the source handed over that the active hook has
	// not consumed yet; PeakInFlight is the high-water mark.
	InFlight     atomic.Int64
	PeakInFlight atomic.Int64
}

// ErrReaderClosedTwice is returned when a tracked source is closed repeatedly.
var ErrReaderClosedTwice = errors.New("conformance: input reader closed more than once")

// trackedReadCloser is the instrumented source given to the public Parse API.
type trackedReadCloser struct {
	mu sync.Mutex

	r     io.Reader
	base  io.Closer // underlying source to release on Close (e.g. io.PipeReader)
	stats *ReaderStats

	closed bool
	done   chan struct{} // closed on first Close

	fail      error
	failAfter int64
	read      int64
	failed    bool
}

func (t *trackedReadCloser) Read(p []byte) (int, error) {
	t.mu.Lock()
	if t.closed {
		err := io.ErrClosedPipe
		if t.failed {
			err = t.fail
		}
		t.mu.Unlock()
		return 0, err
	}
	fail, failAfter, alreadyFailed := t.fail, t.failAfter, t.failed
	t.mu.Unlock()

	if alreadyFailed {
		return 0, fail
	}

	n, err := t.r.Read(p)
	if n > 0 {
		t.stats.BytesRead.Add(int64(n))
		inFlight := t.stats.InFlight.Add(int64(n))
		for {
			peak := t.stats.PeakInFlight.Load()
			if inFlight <= peak || t.stats.PeakInFlight.CompareAndSwap(peak, inFlight) {
				break
			}
		}
	}
	t.stats.ReadCalls.Add(1)

	t.mu.Lock()
	t.read += int64(n)
	if fail != nil && !t.failed && failAfter > 0 && t.read >= failAfter {
		t.failed = true
		if err == nil {
			err = fail
		}
	}
	t.mu.Unlock()

	return n, err
}

// Consumed lowers the in-flight mark when a hook drains bytes.
func (t *trackedReadCloser) Consumed(n int64) { t.stats.InFlight.Add(-n) }

func (t *trackedReadCloser) Close() error {
	t.mu.Lock()
	first := !t.closed
	t.closed = true
	base := t.base
	t.mu.Unlock()

	t.stats.CloseCalls.Add(1)
	if !first {
		return ErrReaderClosedTwice
	}
	close(t.done)
	if base != nil {
		_ = base.Close()
	}
	return nil
}

// Done is closed once the parser releases the source.
func (t *trackedReadCloser) Done() <-chan struct{} { return t.done }

// staticSource serves a materialized body in bounded reads.
type staticSource struct {
	buf *bytes.Buffer
	n   int
}

func (s *staticSource) Read(p []byte) (int, error) {
	if s.buf.Len() == 0 {
		return 0, io.EOF
	}
	size := min(s.n, s.buf.Len())
	return s.buf.Read(p[:min(len(p), size)])
}

// streamingSource receives scripted chunks from a producer goroutine.
type streamingSource struct {
	chunks <-chan []byte
	cur    []byte
}

func (s *streamingSource) Read(p []byte) (int, error) {
	for len(s.cur) == 0 {
		chunk, ok := <-s.chunks
		if !ok {
			return 0, io.EOF
		}
		s.cur = chunk
	}
	n := copy(p, s.cur)
	s.cur = s.cur[n:]
	return n, nil
}

// pumpChunks feeds scripted chunks and exits when drained or stop closes.
func pumpChunks(chunks chan<- []byte, script [][]byte, stop <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(chunks)
		for _, chunk := range script {
			select {
			case chunks <- chunk:
			case <-stop:
				return
			}
		}
	}()
	return done
}

// Source bundles the tracked reader with its producer, if any.
type Source struct {
	r    *trackedReadCloser
	pump <-chan struct{}
}

// NewSource builds the instrumented source described by b.
func (b Body) NewSource() *Source {
	if b.FailAfter > 0 || b.Stream {
		body, pumpDone := b.streamingBody()
		var base io.Closer
		if c, ok := body.(io.Closer); ok {
			base = c
		}
		r := &trackedReadCloser{
			r:         body,
			base:      base,
			stats:     &ReaderStats{},
			done:      make(chan struct{}),
			fail:      b.FailErr,
			failAfter: int64(b.FailAfter),
		}
		return &Source{r: r, pump: pumpDone}
	}

	r := &trackedReadCloser{
		r:     &staticSource{buf: bytes.NewBuffer(b.Build()), n: 4096},
		stats: &ReaderStats{},
		done:  make(chan struct{}),
	}
	return &Source{r: r}
}

// Open returns the io.ReadCloser passed to a public Parse entry point.
func (s *Source) Open() io.ReadCloser { return s.r }

// Stats returns the I/O footprint.
func (s *Source) Stats() *ReaderStats { return s.r.stats }

// PumpDone reports whether the scripted producer goroutine has exited.
func (s *Source) PumpDone() bool {
	if s.pump == nil {
		return true
	}
	select {
	case <-s.pump:
		return true
	default:
		return false
	}
}

// Tracked returns the internal tracker (used for hook in-flight accounting).
func (s *Source) tracked() *trackedReadCloser { return s.r }
