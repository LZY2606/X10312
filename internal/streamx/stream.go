// Package streamx provides the bounded single-producer streaming primitive
// used by every parser execution path (the root parser and the net/http,
// echo and gin adapters all route hook part bodies through Stream).
//
// A Stream runs exactly one producer goroutine that pumps data from the
// underlying io.Reader into a fixed-depth chunk channel. The consumer reads
// from the same goroutine that runs parse. Backpressure is therefore real:
// the producer can hold at most MaxBuffered bytes plus one chunk on behalf of
// a slow consumer, never the whole part.
//
// Lifecycle:
//
//   - Finish tells the producer the consumer stopped early: the producer
//     drains the remainder of the current part so multipart framing stays
//     intact, then stops. It must be called after the consumer hook returns
//     successfully.
//   - Cancel asks the producer to abandon the part immediately (reader
//     failure, hook failure or context cancellation).
//
// The first causal error is preserved; later errors never replace it.
package streamx

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

const (
	// ChunkSize is the size of every buffered chunk.
	ChunkSize = 32 * 1024
	// MaxQueuedChunks bounds the number of chunks the producer may queue.
	MaxQueuedChunks = 16
	// MaxBuffered is the maximum number of payload bytes that may be queued
	// for a slow consumer. Total in-flight memory is bounded by MaxBuffered
	// plus one ChunkSize.
	MaxBuffered = ChunkSize * MaxQueuedChunks
)

// Observer receives streaming-internal observations. It is intentionally
// narrow and reachable only through the internal context seam so production
// behavior is unchanged when no observer is attached.
type Observer interface {
	OnBuffered(inFlightBytes int64)
}

type observerContextKey struct{}

// WithObserver attaches an Observer to ctx.
func WithObserver(ctx context.Context, observer Observer) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, observerContextKey{}, observer)
}

func observerFromContext(ctx context.Context) Observer {
	observer, _ := ctx.Value(observerContextKey{}).(Observer)
	return observer
}

// Chunk pool counters let callers verify every borrowed chunk is returned,
// including on reader failure, consumer stop and cancellation.
var (
	chunkPool = sync.Pool{
		New: func() any {
			b := make([]byte, ChunkSize)
			return &b
		},
	}
	poolGets atomic.Int64
	poolPuts atomic.Int64
)

// PoolStats reports the cumulative number of chunks borrowed from and
// returned to the shared pool. A finished run must show equal deltas.
func PoolStats() (gets, puts int64) {
	return poolGets.Load(), poolPuts.Load()
}

func getChunk() *[]byte {
	poolGets.Add(1)
	return chunkPool.Get().(*[]byte)
}

func putChunk(b *[]byte) {
	if b == nil {
		return
	}
	poolPuts.Add(1)
	chunkPool.Put(b)
}

// Stream is a bounded single-producer wrapper around an io.Reader.
type Stream struct {
	src io.Reader
	ctx context.Context
	obs Observer

	ch     chan *[]byte
	finish chan struct{}
	cancel chan struct{}

	startOnce sync.Once
	started   atomic.Bool
	finishOnce sync.Once
	cancelOnce sync.Once
	pumpDone  chan struct{}
	closeChOnce sync.Once

	errMu     sync.Mutex
	firstErr  error
	buffered  atomic.Int64
	peak      atomic.Int64

	current *[]byte
	offset  int
}

// New creates a Stream reading from src under ctx. The producer goroutine is
// started lazily on the first Read, so readers that are consumed elsewhere
// (the buffered abnormal path) never spawn one.
func New(src io.Reader, ctx context.Context) *Stream {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Stream{
		src:    src,
		ctx:    ctx,
		obs:    observerFromContext(ctx),
		ch:     make(chan *[]byte, MaxQueuedChunks),
		finish: make(chan struct{}),
		cancel: make(chan struct{}),
		pumpDone: make(chan struct{}),
	}
}

// PeakBuffered reports the high-water mark of bytes queued but not yet
// consumed.
func (s *Stream) PeakBuffered() int64 {
	return s.peak.Load()
}

func (s *Stream) start() {
	s.started.Store(true)
	if s.ctx.Done() != nil {
		stopAfterFunc := context.AfterFunc(s.ctx, s.Cancel)
		go func() {
			defer stopAfterFunc()
			s.pump()
		}()
		return
	}
	go s.pump()
}

// Read implements io.Reader.
func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.startOnce.Do(s.start)

	if s.current == nil {
		chunk, err := s.nextChunk()
		if err != nil {
			return 0, err
		}
		s.current = chunk
		s.offset = 0
	}

	n := copy(p, (*s.current)[s.offset:])
	s.offset += n
	if s.offset >= len(*s.current) {
		s.addBuffered(-int64(len(*s.current)))
		putChunk(s.current)
		s.current = nil
	}

	return n, nil
}

func (s *Stream) nextChunk() (*[]byte, error) {
	for {
		select {
		case <-s.ctx.Done():
			s.Cancel()
			continue
		case <-s.cancel:
			return nil, s.causalError()
		case chunk, ok := <-s.ch:
			if !ok {
				if err := s.causalError(); err != nil {
					return nil, err
				}
				return nil, io.EOF
			}
			return chunk, nil
		}
	}
}

func (s *Stream) causalError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.firstErr != nil {
		return s.firstErr
	}
	if err := s.ctx.Err(); err != nil {
		if cause := context.Cause(s.ctx); cause != nil {
			return cause
		}
		return err
	}
	return io.EOF
}

func (s *Stream) pump() {
	defer close(s.pumpDone)

	stopping := false
	for {
		if stopping {
			// Consumer finished early: discard the remainder of the part so
			// the surrounding multipart reader still sees its boundary.
			chunk := getChunk()
			_, readErr := s.src.Read((*chunk)[:ChunkSize])
			putChunk(chunk)
			if readErr != nil {
				if !errors.Is(readErr, io.EOF) {
					s.recordError(readErr)
				}
				s.closeCh()
				return
			}
			continue
		}

		chunk := getChunk()
		n, readErr := s.src.Read((*chunk)[:ChunkSize])
		if n > 0 {
			*chunk = (*chunk)[:n]
			select {
			case s.ch <- chunk:
				s.addBuffered(int64(n))
			case <-s.cancel:
				putChunk(chunk)
				return
			case <-s.finish:
				// chunk is already queued; Finish reclaims it after the
				// producer stops. Enter draining mode without re-putting it.
				stopping = true
			}
		} else {
			putChunk(chunk)
		}

		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				s.recordError(readErr)
			}
			s.closeCh()
			return
		}
	}
}

// Finish marks the consumer side as done and waits for the producer to stop.
// The remaining part is drained when the consumer stopped early; queued and
// in-flight chunks are reclaimed. It is safe to call after Cancel.
func (s *Stream) Finish() {
	s.finishOnce.Do(func() { close(s.finish) })
	if s.started.Load() {
		<-s.pumpDone
	}

	for {
		select {
		case chunk, ok := <-s.ch:
			if !ok {
				goto reclaimed
			}
			s.addBuffered(-int64(len(*chunk)))
			putChunk(chunk)
		default:
			goto reclaimed
		}
	}
reclaimed:
	if s.current != nil {
		s.addBuffered(-int64(len(*s.current)))
		putChunk(s.current)
		s.current = nil
	}
}

// Cancel asks the producer to stop without draining. A context error, if the
// context is done, becomes the causal error; a hook/reader-side error is
// recorded by the caller of the surrounding parse.
func (s *Stream) Cancel() {
	s.cancelOnce.Do(func() {
		if err := s.ctx.Err(); err != nil {
			if cause := context.Cause(s.ctx); cause != nil {
				err = cause
			}
			s.recordError(err)
		}
		close(s.cancel)
	})
}

func (s *Stream) closeCh() {
	s.closeChOnce.Do(func() { close(s.ch) })
}

func (s *Stream) recordError(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.errMu.Unlock()
}

func (s *Stream) addBuffered(delta int64) {
	current := s.buffered.Add(delta)
	if delta > 0 {
		for {
			peak := s.peak.Load()
			if current <= peak || s.peak.CompareAndSwap(peak, current) {
				break
			}
		}
	}
	if s.obs != nil {
		s.obs.OnBuffered(current)
	}
}
