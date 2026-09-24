package conformance_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/labstack/echo/v4"
	"github.com/mazrean/formstream"
	echoform "github.com/mazrean/formstream/echo"
	ginform "github.com/mazrean/formstream/gin"
	httpform "github.com/mazrean/formstream/http"
)

// testBoundary is the single boundary used by every conformance case so
// that bodies are reproducible byte-for-byte across adapters and retries.
const testBoundary = "conformance-boundary-7d1a9f"

// Sentinel causal errors used by the negative fixtures. They must survive
// the parser's error wrapping chain so callers can errors.Is them.
var (
	errBoom  = errors.New("conformance: injected reader failure")
	errHook  = errors.New("conformance: consumer stopped early")
	errFlaky = errors.New("conformance: recoverable mid-stream failure")
)

// parseHandle unifies the four public adapters behind the small surface
// the harness needs. Every factory goes through the adapter's public
// constructor; nothing reaches into formstream internals.
type parseHandle struct {
	register func(name string, fn formstream.StreamHookFunc, opts ...formstream.RegisterOption) error
	parse    func() error
	valueMap func() map[string][]formstream.Value
}

type adapterFactory struct {
	name      string
	newHandle func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parseHandle, error)
}

func coreHandle(p *formstream.Parser, parse func() error) *parseHandle {
	return &parseHandle{
		register: p.Register,
		parse:    parse,
		valueMap: p.ValueMap,
	}
}

func adapters() []adapterFactory {
	return []adapterFactory{
		{
			name: "core",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parseHandle, error) {
				p := formstream.NewParser(boundary, opts...)
				return coreHandle(p, func() error { return p.Parse(body) }), nil
			},
		},
		{
			name: "http",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parseHandle, error) {
				req := multipartRequest(body, boundary)
				p, err := httpform.NewParser(req, opts...)
				if err != nil {
					return nil, fmt.Errorf("httpform.NewParser: %w", err)
				}
				return coreHandle(p.Parser, p.Parse), nil
			},
		},
		{
			name: "gin",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parseHandle, error) {
				gin.SetMode(gin.TestMode)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = multipartRequest(body, boundary)
				p, err := ginform.NewParser(c, opts...)
				if err != nil {
					return nil, fmt.Errorf("ginform.NewParser: %w", err)
				}
				return coreHandle(p.Parser, p.Parse), nil
			},
		},
		{
			name: "echo",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parseHandle, error) {
				e := echo.New()
				c := e.NewContext(multipartRequest(body, boundary), httptest.NewRecorder())
				p, err := echoform.NewParser(c, opts...)
				if err != nil {
					return nil, fmt.Errorf("echoform.NewParser: %w", err)
				}
				return coreHandle(p.Parser, p.Parse), nil
			},
		},
	}
}

func adapterByName(t *testing.T, name string) adapterFactory {
	t.Helper()
	for _, a := range adapters() {
		if a.name == name {
			return a
		}
	}
	t.Fatalf("unknown adapter %q", name)
	return adapterFactory{}
}

func multipartRequest(body io.Reader, boundary string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/conformance", body)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	return req
}

// countingReader is the harness instrumentation. It only observes: read
// calls, bytes, and (via countingCloser) closes. It never alters the
// stream, which TestConformance_InstrumentationTransparent proves.
type countingReader struct {
	r         io.Reader
	reads     atomic.Int64
	bytesRead atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.reads.Add(1)
	c.bytesRead.Add(int64(n))
	return n, err
}

type countingReadCloser struct {
	*countingReader
	closeFn func() error
	closes  atomic.Int64
}

func (c *countingReadCloser) Close() error {
	c.closes.Add(1)
	return c.closeFn()
}

// inflightTracker records the peak number of bytes produced into the
// stream but not yet consumed by the hook: the observable "peak buffer"
// of the pipeline. A read-everything-first implementation is caught by
// this metric because its peak approaches the full file size.
type inflightTracker struct {
	written  atomic.Int64
	consumed atomic.Int64
	peak     atomic.Int64
}

func (t *inflightTracker) addWritten(n int64) {
	t.written.Add(n)
	t.sample()
}

func (t *inflightTracker) addConsumed(n int64) {
	t.consumed.Add(n)
	t.sample()
}

func (t *inflightTracker) sample() {
	cur := t.written.Load() - t.consumed.Load()
	for {
		peak := t.peak.Load()
		if cur <= peak || t.peak.CompareAndSwap(peak, cur) {
			return
		}
	}
}

type trackingWriter struct {
	w       io.Writer
	tracker *inflightTracker
}

func (tw *trackingWriter) Write(p []byte) (int, error) {
	n, err := tw.w.Write(p)
	tw.tracker.addWritten(int64(n))
	return n, err
}

type trackingReader struct {
	r       io.Reader
	tracker *inflightTracker
}

func (tr *trackingReader) Read(p []byte) (int, error) {
	n, err := tr.r.Read(p)
	tr.tracker.addConsumed(int64(n))
	return n, err
}

// producer streams a multipart body through an io.Pipe from its own
// goroutine, so the harness never materializes the whole body in memory.
type producer struct {
	reader *io.PipeReader
	done   chan error
}

func newProducer(tracker *inflightTracker, write func(w io.Writer) error) *producer {
	pr, pw := io.Pipe()
	p := &producer{reader: pr, done: make(chan error, 1)}
	go func() {
		err := write(&trackingWriter{w: pw, tracker: tracker})
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			err = pw.Close()
		}
		p.done <- err
	}()
	return p
}

// release unblocks a producer still waiting on the pipe (e.g. after the
// consumer stopped early) and reports the parse error as the cause.
func (p *producer) release(cause error) {
	_ = p.reader.CloseWithError(cause)
}

// wait asserts the producer goroutine exits, which is the leak gate for
// every case that stops the stream mid-way.
func (p *producer) wait(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("producer goroutine did not exit after the stream was released")
	}
}

// settleGoroutines returns the goroutine count after letting the runtime
// quiesce; used as the baseline for the leak gate.
func settleGoroutines() int {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	return runtime.NumGoroutine()
}

// assertGoroutinesSettled is the goroutine regression gate: after the
// workload, the count must fall back to the baseline.
func assertGoroutinesSettled(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak gate: baseline=%d, current=%d", baseline, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// assertNoFormstreamTempFiles asserts the parser released every spilled
// temp file (preProcessor cleanup) inside dir.
func assertNoFormstreamTempFiles(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "formstream-*"))
	if err != nil {
		t.Fatalf("glob temp dir: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("leaked formstream temp files: %v", matches)
	}
}

// patternReader is an infinite deterministic byte source, so multi-MB
// fixtures are generated on the fly instead of being held in memory.
type patternReader struct {
	off int64
}

func (p *patternReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte((p.off+int64(i))*167 + 13)
	}
	p.off += int64(len(b))
	return len(b), nil
}

func hashPattern(size int64) string {
	h := sha256.New()
	_, _ = io.CopyN(h, &patternReader{}, size)
	return hex.EncodeToString(h.Sum(nil))
}

func patternBytes(size int64) []byte {
	buf := make([]byte, size)
	_, _ = (&patternReader{}).Read(buf)
	return buf
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// fileEvent is one observable hook invocation, reduced to comparable data.
type fileEvent struct {
	Name        string
	FileName    string
	ContentType string
	Size        int64
	SHA256      string
}

// canonicalResult is what the caller can observe after a successful
// parse, normalized so results are comparable across adapters and runs.
type canonicalResult struct {
	Values map[string][]string
	Files  []fileEvent
}

func collectValues(p *parseHandle) map[string][]string {
	values := make(map[string][]string)
	for key, vals := range p.valueMap() {
		for _, v := range vals {
			content, _ := v.Unwrap()
			values[key] = append(values[key], content)
		}
	}
	return values
}

// consumeKind describes how a hook consumes its part reader.
type consumeKind int

const (
	consumeDrain consumeKind = iota
	consumePrefix
	consumeFailAfter
)

type hookSpec struct {
	name         string
	requireParts []string
	kind         consumeKind
	limit        int64
	slowPerRead  time.Duration
}

// buildHook turns a hookSpec into a public StreamHookFunc that records
// each invocation as a fileEvent. The optional tracker observes the
// in-flight window for the peak-buffer invariant.
func (s hookSpec) buildHook(events *[]fileEvent, tracker *inflightTracker, onEntry func()) formstream.StreamHookFunc {
	return func(r io.Reader, header formstream.Header) error {
		if onEntry != nil {
			onEntry()
		}
		if tracker != nil {
			r = &trackingReader{r: r, tracker: tracker}
		}
		if s.slowPerRead > 0 {
			r = &slowReader{r: r, delay: s.slowPerRead}
		}

		h := sha256.New()
		var size int64
		switch s.kind {
		case consumeDrain:
			n, err := io.Copy(h, r)
			size = n
			if err != nil {
				return fmt.Errorf("hook drain: %w", err)
			}
		case consumePrefix, consumeFailAfter:
			n, err := io.CopyN(h, r, s.limit)
			size = n
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("hook prefix read: %w", err)
			}
			if s.kind == consumeFailAfter {
				return errHook
			}
		}

		*events = append(*events, fileEvent{
			Name:        header.Name(),
			FileName:    header.FileName(),
			ContentType: header.ContentType(),
			Size:        size,
			SHA256:      hex.EncodeToString(h.Sum(nil)),
		})
		return nil
	}
}

type slowReader struct {
	r     io.Reader
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.r.Read(p)
}

// partSpec describes one part of a fixture body.
type partSpec struct {
	name         string
	fileName     string
	contentType  string
	extraHeaders [][2]string
	content      []byte
}

// buildBody renders parts into a deterministic multipart body. Filenames
// go through mime.FormatMediaType so unicode names are RFC 2231 encoded,
// exactly like a real client would send them.
func buildBody(t *testing.T, parts []partSpec) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	mw := multipart.NewWriter(buf)
	if err := mw.SetBoundary(testBoundary); err != nil {
		t.Fatalf("set boundary: %v", err)
	}
	for _, part := range parts {
		params := map[string]string{"name": part.name}
		if part.fileName != "" {
			params["filename"] = part.fileName
		}
		hdr := textproto.MIMEHeader{}
		hdr.Set("Content-Disposition", mime.FormatMediaType("form-data", params))
		if part.contentType != "" {
			hdr.Set("Content-Type", part.contentType)
		}
		for _, kv := range part.extraHeaders {
			hdr.Add(kv[0], kv[1])
		}
		pw, err := mw.CreatePart(hdr)
		if err != nil {
			t.Fatalf("create part %q: %v", part.name, err)
		}
		if _, err := pw.Write(part.content); err != nil {
			t.Fatalf("write part %q: %v", part.name, err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return buf.Bytes()
}

// failAfterReader returns err after n bytes, simulating a source that
// dies mid-stream.
type failAfterReader struct {
	r    io.Reader
	left int64
	err  error
}

func (f *failAfterReader) Read(p []byte) (int, error) {
	if f.left <= 0 {
		return 0, f.err
	}
	if int64(len(p)) > f.left {
		p = p[:f.left]
	}
	n, err := f.r.Read(p)
	f.left -= int64(n)
	if err != nil {
		return n, err
	}
	if f.left == 0 {
		return n, nil
	}
	return n, nil
}

// runFixture executes one fixture through one adapter and returns the
// observable result plus the parse error. Instrumentation is optional so
// tests can prove it does not change successful outcomes.
func runFixture(t *testing.T, adapter adapterFactory, fx fixture, instrument bool) (canonicalResult, error) {
	t.Helper()

	body := buildBody(t, fx.parts)
	var reader io.Reader = bytes.NewReader(body)
	if fx.corruptAfter > 0 {
		reader = &failAfterReader{r: reader, left: fx.corruptAfter, err: errBoom}
	}
	var counter *countingReader
	if instrument {
		counter = &countingReader{r: reader}
		reader = counter
	}

	handle, err := adapter.newHandle(reader, testBoundary, fx.parserOpts...)
	if err != nil {
		t.Fatalf("adapter %s: %v", adapter.name, err)
	}

	var events []fileEvent
	for _, spec := range fx.hooks {
		hook := spec.buildHook(&events, nil, nil)
		var opts []formstream.RegisterOption
		for _, req := range spec.requireParts {
			opts = append(opts, formstream.WithRequiredPart(req))
		}
		if err := handle.register(spec.name, hook, opts...); err != nil {
			t.Fatalf("adapter %s: register hook %q: %v", adapter.name, spec.name, err)
		}
	}

	parseErr := handle.parse()

	result := canonicalResult{Values: collectValues(handle), Files: events}
	if result.Files == nil {
		result.Files = []fileEvent{}
	}
	for key := range result.Values {
		if result.Values[key] == nil {
			result.Values[key] = []string{}
		}
	}

	if counter != nil {
		t.Logf("adapter=%s fixture=%s reads=%d bytes=%d",
			adapter.name, fx.name, counter.reads.Load(), counter.bytesRead.Load())
	}

	return result, parseErr
}

// fixture is one conformance case: a fixed input plus the observable
// contract it must satisfy through every adapter.
type fixture struct {
	name         string
	parts        []partSpec
	parserOpts   []formstream.ParserOption
	hooks        []hookSpec
	corruptAfter int64
	want         canonicalResult
	wantErr      error
}

// tempDirForSpills points os.TempDir at a private directory so the
// resource-release tests can observe formstream's spill files without
// interference from other test binaries running in parallel.
func tempDirForSpills(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	return dir
}
