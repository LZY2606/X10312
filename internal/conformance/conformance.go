// Package conformance provides the shared "formstream: cross-adapter
// streaming consistency and leak gate" harness.
//
// The harness only ever enters through the repository's public entry points
// (formstream.NewParser/Register/Parse and the net/http, gin, echo adapter
// NewParser constructors). It never fabricates internal final objects and it
// does not contain a test-only copy of the parsing algorithm: every case
// exercises the exact production path
// (Parser.Parse -> hookSatisfactionChecker -> preProcessor -> hooks), only
// the transport feeding Parser.Parse and the observers around it are added
// by the harness.
package conformance

import (
	"bytes"
	"context"
	"errors"
	"hash/fnv"
	"io"
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

// Boundary is the multipart boundary used by every conformance case.
const Boundary = "conformance-boundary"

// Parser is the public surface shared by the core parser and every adapter.
type Parser interface {
	Register(name string, fn formstream.StreamHookFunc, options ...formstream.RegisterOption) error
	Value(key string) (string, formstream.Header, bool)
	Values(key string) ([]formstream.Value, bool)
}

// Factory builds a parser over src through one public adapter entry point and
// returns the parser together with its parse action.
type Factory func(src io.Reader, opts ...formstream.ParserOption) (Parser, func() error, error)

// Adapter names a public entry point and knows how to build parsers through it.
type Adapter struct {
	Name    string
	Factory Factory
}

// CoreAdapter drives the root formstream package directly.
func CoreAdapter() Adapter {
	return Adapter{
		Name: "core",
		Factory: func(src io.Reader, opts ...formstream.ParserOption) (Parser, func() error, error) {
			p := formstream.NewParser(Boundary, opts...)
			return p, func() error { return p.Parse(src) }, nil
		},
	}
}

// Metrics records the observable I/O of a single case: read/close counts on
// the source, the largest single read chunk, and the peak in-flight buffer
// (bytes produced but not yet consumed by hooks).
type Metrics struct {
	ReadCalls    atomic.Int64
	BytesRead    atomic.Int64
	CloseCalls   atomic.Int64
	MaxChunk     atomic.Int64
	Written      atomic.Int64
	Consumed     atomic.Int64
	PeakInflight atomic.Int64
}

// AddConsumed records n bytes consumed by a hook.
func (m *Metrics) AddConsumed(n int64) {
	m.Consumed.Add(n)
}

func (m *Metrics) observeInflight() {
	inflight := m.Written.Load() - m.Consumed.Load()
	for {
		old := m.PeakInflight.Load()
		if inflight <= old || m.PeakInflight.CompareAndSwap(old, inflight) {
			return
		}
	}
}

// Log reports the recorded counters with the case label.
func (m *Metrics) Log(t *testing.T, label string) {
	t.Helper()
	t.Logf("%s: reads=%d bytesRead=%d closes=%d maxChunk=%d written=%d consumed=%d peakInflight=%d",
		label,
		m.ReadCalls.Load(), m.BytesRead.Load(), m.CloseCalls.Load(), m.MaxChunk.Load(),
		m.Written.Load(), m.Consumed.Load(), m.PeakInflight.Load())
}

// Source wraps the case input and records read/close counts and chunk sizes.
// It can inject a recoverable failure once a byte budget has been served.
type Source struct {
	r         io.Reader
	c         io.Closer
	m         *Metrics
	failAfter int64
	failErr   error
}

// NewSource wraps r. If r is an io.Closer its Close is delegated to (and
// counted); otherwise Close is a counted no-op.
func NewSource(r io.Reader, m *Metrics) *Source {
	src := &Source{r: r, m: m, failAfter: -1}
	if c, ok := r.(io.Closer); ok {
		src.c = c
	}
	return src
}

// FailAfter makes the source return err once at least n bytes have been
// served, simulating a recoverable mid-stream reader failure.
func (s *Source) FailAfter(n int64, err error) *Source {
	s.failAfter = n
	s.failErr = err
	return s
}

func (s *Source) Read(p []byte) (int, error) {
	if s.failErr != nil && s.m.BytesRead.Load() >= s.failAfter {
		return 0, s.failErr
	}
	n, err := s.r.Read(p)
	s.m.ReadCalls.Add(1)
	s.m.BytesRead.Add(int64(n))
	for {
		old := s.m.MaxChunk.Load()
		if int64(n) <= old || s.m.MaxChunk.CompareAndSwap(old, int64(n)) {
			break
		}
	}
	return n, err
}

func (s *Source) Close() error {
	s.m.CloseCalls.Add(1)
	if s.c != nil {
		return s.c.Close()
	}
	return nil
}

// Producer streams a source into an io.Pipe from a dedicated goroutine so the
// parser consumes it the way an HTTP server consumes a request body. The
// goroutine exits on source EOF, source error, context cancellation, or
// consumer stop, and keeps the first causal error.
type Producer struct {
	reader *io.PipeReader
	done   chan struct{}

	mu       sync.Mutex
	firstErr error
}

// StartProducer starts the producer goroutine. chunk is the read/write chunk
// size and delay, when positive, throttles each chunk to perturb scheduling.
func StartProducer(ctx context.Context, src io.Reader, m *Metrics, chunk int, delay time.Duration) *Producer {
	pr, pw := io.Pipe()
	p := &Producer{reader: pr, done: make(chan struct{})}

	go func() {
		defer close(p.done)
		buf := make([]byte, chunk)
		for {
			select {
			case <-ctx.Done():
				p.setErr(ctx.Err())
				_ = pw.CloseWithError(ctx.Err())
				return
			default:
			}

			n, rerr := src.Read(buf)
			if n > 0 {
				if delay > 0 {
					time.Sleep(delay)
				}
				wn, werr := pw.Write(buf[:n])
				m.Written.Add(int64(wn))
				m.observeInflight()
				if werr != nil {
					// Consumer stopped early; the pipe write error is the
					// producer's own causal error.
					p.setErr(werr)
					return
				}
			}
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					_ = pw.Close()
				} else {
					p.setErr(rerr)
					_ = pw.CloseWithError(rerr)
				}
				return
			}
		}
	}()

	return p
}

func (p *Producer) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.firstErr == nil {
		p.firstErr = err
	}
}

// FirstErr returns the first causal error observed by the producer goroutine.
func (p *Producer) FirstErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstErr
}

// Reader is the reader endpoint handed to the parser.
func (p *Producer) Reader() io.Reader {
	return p.reader
}

// Close releases the producer when the consumer (parser) stopped early.
func (p *Producer) Close(err error) {
	_ = p.reader.CloseWithError(err)
}

// Wait blocks until the producer goroutine exits or the timeout elapses.
func (p *Producer) Wait(timeout time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Part describes one multipart part of a case body.
type Part struct {
	Name        string
	FileName    string
	ContentType string
	Headers     map[string]string
	Content     []byte
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

// BuildBody encodes parts as a multipart body using the standard library
// writer, exactly like a real HTTP client would.
func BuildBody(parts ...Part) []byte {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if err := w.SetBoundary(Boundary); err != nil {
		panic(err)
	}
	for _, pt := range parts {
		h := textproto.MIMEHeader{}
		cd := `form-data; name="` + quoteEscaper.Replace(pt.Name) + `"`
		if pt.FileName != "" {
			cd += `; filename="` + quoteEscaper.Replace(pt.FileName) + `"`
		}
		h.Set("Content-Disposition", cd)
		ct := pt.ContentType
		if ct == "" && pt.FileName != "" {
			ct = "application/octet-stream"
		}
		if ct != "" {
			h.Set("Content-Type", ct)
		}
		for k, v := range pt.Headers {
			h.Set(k, v)
		}
		pw, err := w.CreatePart(h)
		if err != nil {
			panic(err)
		}
		if _, err := pw.Write(pt.Content); err != nil {
			panic(err)
		}
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// payloadReader deterministically generates n payload bytes without holding
// them in memory, so the harness itself never buffers a whole large file.
type payloadReader struct {
	i, n int64
}

func payloadByte(i int64) byte {
	return byte((i * 31) ^ (i >> 8) ^ (i >> 16))
}

func (r *payloadReader) Read(p []byte) (int, error) {
	if r.i >= r.n {
		return 0, io.EOF
	}
	m := min(int64(len(p)), r.n-r.i)
	for j := int64(0); j < m; j++ {
		p[j] = payloadByte(r.i + j)
	}
	r.i += m
	return int(m), nil
}

// StreamingFileBody emits a multipart body with a single file part of size
// generated bytes followed by tail parts, without buffering the payload.
func StreamingFileBody(name, fileName string, size int64, tail ...Part) io.Reader {
	head := "--" + Boundary + "\r\n" +
		`Content-Disposition: form-data; name="` + quoteEscaper.Replace(name) +
		`"; filename="` + quoteEscaper.Replace(fileName) + `"` + "\r\n" +
		"Content-Type: application/octet-stream\r\n\r\n"
	tailBytes := append([]byte("\r\n"), BuildBody(tail...)...)
	return io.MultiReader(strings.NewReader(head), &payloadReader{n: size}, bytes.NewReader(tailBytes))
}

// PayloadHash recomputes the expected FNV-1a hash of a generated payload.
func PayloadHash(size int64) uint64 {
	h := fnv.New64a()
	_, _ = io.Copy(h, &payloadReader{n: size})
	return h.Sum64()
}

// HeapSampler samples runtime heap usage in the background and keeps the peak.
type HeapSampler struct {
	peak atomic.Uint64
	stop chan struct{}
	done chan struct{}
}

// StartHeapSampler starts sampling Alloc at the given interval.
func StartHeapSampler(interval time.Duration) *HeapSampler {
	s := &HeapSampler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				for {
					old := s.peak.Load()
					if ms.Alloc <= old || s.peak.CompareAndSwap(old, ms.Alloc) {
						break
					}
				}
			}
		}
	}()
	return s
}

// Stop ends the sampling goroutine.
func (s *HeapSampler) Stop() {
	close(s.stop)
	<-s.done
}

// Peak returns the peak sampled Alloc in bytes.
func (s *HeapSampler) Peak() uint64 {
	return s.peak.Load()
}

// TempFiles snapshots the parser's temporary files (os.TempDir/formstream-*).
func TempFiles(t *testing.T) map[string]struct{} {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "formstream-*"))
	if err != nil {
		t.Fatalf("conformance: failed to glob temp files: %v", err)
	}
	set := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		set[m] = struct{}{}
	}
	return set
}

// AssertNoNewTempFiles fails the test if files exist now that were not in
// before, proving the parser released its temporary state.
func AssertNoNewTempFiles(t *testing.T, before map[string]struct{}) {
	t.Helper()
	after := TempFiles(t)
	for f := range after {
		if _, ok := before[f]; !ok {
			t.Errorf("conformance: temp file leak gate violated: %q remains after Parse returned", f)
		}
	}
}

// AssertGoroutinesSettled is the goroutine leak gate: it waits until the
// goroutine count returns to baseline and reports the first violation with
// the offending delta otherwise.
func AssertGoroutinesSettled(t *testing.T, baseline int, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("conformance: goroutine leak gate violated at %s: baseline=%d current=%d (%d goroutine(s) did not exit)",
				label, baseline, n, n-baseline)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// FinishProducer enforces the producer-exit gate after Parse returned: the
// consumer-side close releases a blocked producer, which must then exit.
func FinishProducer(t *testing.T, a Adapter, p *Producer, parseErr error) {
	t.Helper()
	p.Close(parseErr)
	if !p.Wait(5 * time.Second) {
		t.Errorf("conformance: producer goroutine did not exit after Parse returned (adapter=%s)", a.Name)
	}
}

// AssertSourceNotClosed enforces that the parser never closes the caller's
// source: ownership of the request body stays with the caller.
func AssertSourceNotClosed(t *testing.T, a Adapter, m *Metrics) {
	t.Helper()
	if got := m.CloseCalls.Load(); got != 0 {
		t.Errorf("conformance: parser closed the caller's source %d time(s); the source belongs to the caller (adapter=%s)", got, a.Name)
	}
}

// NewMetricsSource pairs a fresh Metrics with a Source over r.
func NewMetricsSource(r io.Reader) (*Metrics, *Source) {
	m := &Metrics{}
	return m, NewSource(r, m)
}

// Sentinel errors used by the cases to verify causal-error preservation.
var (
	// ErrInjected is the recoverable mid-stream reader failure.
	ErrInjected = errors.New("conformance: injected reader failure")
	// ErrConsumerAbort is returned by a hook that reads only a prefix and aborts.
	ErrConsumerAbort = errors.New("conformance: consumer aborted after prefix")
)
