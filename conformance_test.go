package formstream_test

// Conformance harness for:
//
//	"formstream: cross-adapter streaming consistency and leakage gate"
//
// Every case enters through a formal public adapter:
// formstream.NewParser + Parser.ParseContext, or the http/echo/gin
// NewParser + ParseContext. Internal parser objects are never fabricated.
// The same fixed inputs are replayed through every adapter, then replayed
// again under harmless variations (part ordering, hook registration order,
// delivery chunk size, scheduling delay): the observable outcome must be
// identical across adapters and variations.
//
// The cases also assert streaming/resource invariants: the form body is
// produced incrementally through an io.Pipe (the whole large file is never
// buffered), Read/Close call counts and the largest single Read are bounded,
// and reader failure, consumer early stop and context cancellation all make
// the production goroutines exit while the first causal error is preserved.

import (
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"runtime"
	"strings"
	"sync"
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

const conformanceBoundary = "conformance-boundary"

// ---------------------------------------------------------------------------
// Adapters under test
// ---------------------------------------------------------------------------

type registerHandle struct {
	register func(name string, fn formstream.StreamHookFunc, opts ...formstream.RegisterOption) error
	lookup   func(key string) (string, formstream.Header, bool)
}

type conformanceAdapter struct {
	name  string
	parse func(ctx context.Context, body io.ReadCloser, opts []formstream.ParserOption, hooks func(*registerHandle)) error
	t     *testing.T
}

func adapters(t *testing.T) []conformanceAdapter {
	gin.SetMode(gin.ReleaseMode)
	echoEngine := echo.New()

	return []conformanceAdapter{
		{
			name: "formstream",
			t:    t,
			parse: func(ctx context.Context, body io.ReadCloser, opts []formstream.ParserOption, hooks func(*registerHandle)) error {
				parser := formstream.NewParser(conformanceBoundary, opts...)
				hooks(&registerHandle{register: parser.Register, lookup: parser.Value})
				return parser.ParseContext(ctx, body)
			},
		},
		{
			name: "nethttp",
			t:    t,
			parse: func(ctx context.Context, body io.ReadCloser, opts []formstream.ParserOption, hooks func(*registerHandle)) error {
				req := newConformanceRequest(ctx, body)
				parser, err := httpform.NewParser(req, opts...)
				if err != nil {
					t.Fatalf("http.NewParser: %v", err)
				}
				hooks(&registerHandle{register: parser.Register, lookup: parser.Value})
				return parser.ParseContext(req.Context())
			},
		},
		{
			name: "echo",
			t:    t,
			parse: func(ctx context.Context, body io.ReadCloser, opts []formstream.ParserOption, hooks func(*registerHandle)) error {
				req := newConformanceRequest(ctx, body)
				c := echoEngine.NewContext(req, httptest.NewRecorder())
				parser, err := echoform.NewParser(c, opts...)
				if err != nil {
					t.Fatalf("echo.NewParser: %v", err)
				}
				hooks(&registerHandle{register: parser.Register, lookup: parser.Value})
				return parser.ParseContext(c.Request().Context())
			},
		},
		{
			name: "gin",
			t:    t,
			parse: func(ctx context.Context, body io.ReadCloser, opts []formstream.ParserOption, hooks func(*registerHandle)) error {
				req := newConformanceRequest(ctx, body)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = req
				parser, err := ginform.NewParser(c, opts...)
				if err != nil {
					t.Fatalf("gin.NewParser: %v", err)
				}
				hooks(&registerHandle{register: parser.Register, lookup: parser.Value})
				return parser.ParseContext(req.Context())
			},
		},
	}
}

func newConformanceRequest(ctx context.Context, body io.ReadCloser) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+conformanceBoundary)
	return req.WithContext(ctx)
}

// ---------------------------------------------------------------------------
// Streaming form fixture
// ---------------------------------------------------------------------------

var (
	errFixtureReader       = errors.New("conformance: injected reader failure")
	errFixtureReaderClosed = errors.New("conformance: source read after close")
	errConsumerAborted     = errors.New("conformance: consumer stopped after prefix")
)

// streamForm is a multipart/form-data body produced incrementally through an
// io.Pipe: nothing about the (potentially multi-megabyte) body is retained in
// memory. Writes block until the parser drains them, which naturally
// exercises backpressure against slow consumers.
type streamForm struct {
	pr *io.PipeReader
	pw *io.PipeWriter

	readCalls  atomic.Int64
	closeCalls atomic.Int64
	bytesRead  atomic.Int64
	maxRead    atomic.Int64 // largest single Read delivered to the parser

	producerDone chan struct{}
}

func newStreamForm() *streamForm {
	pr, pw := io.Pipe()
	return &streamForm{pr: pr, pw: pw, producerDone: make(chan struct{})}
}

// produce runs script in a producer goroutine. To inject a recoverable
// mid-stream failure the script closes the writer with errFixtureReader
// instead of finishing the form.
func (f *streamForm) produce(script func(mw *multipart.Writer) error) {
	mw := multipart.NewWriter(f.pw)
	if err := mw.SetBoundary(conformanceBoundary); err != nil {
		_ = f.pw.CloseWithError(err)
		close(f.producerDone)
		return
	}
	go func() {
		defer close(f.producerDone)
		err := script(mw)
		if err == nil {
			err = mw.Close()
		}
		if err == nil {
			_ = f.pw.Close()
		} else {
			_ = f.pw.CloseWithError(err)
		}
	}()
}

func (f *streamForm) Read(p []byte) (int, error) {
	n, err := f.pr.Read(p)
	if n > 0 {
		f.readCalls.Add(1)
		f.bytesRead.Add(int64(n))
		for {
			cur := f.maxRead.Load()
			if int64(n) <= cur || f.maxRead.CompareAndSwap(cur, int64(n)) {
				break
			}
		}
	}
	return n, err
}

func (f *streamForm) Close() error {
	f.closeCalls.Add(1)
	return f.pr.Close()
}

func (f *streamForm) waitProducer(t *testing.T) {
	t.Helper()
	select {
	case <-f.producerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("conformance: form producer goroutine did not exit")
	}
}

// chunkWriter forces every Write through w to be split into pieces of at most
// n bytes, so delivery scheduling can be varied without changing bytes.
type chunkWriter struct {
	w io.Writer
	n int
}

func (c chunkWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		m := len(p)
		if m > c.n {
			m = c.n
		}
		n, err := c.w.Write(p[:m])
		written += n
		if err != nil {
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}

func scriptWriteField(mw *multipart.Writer, name, value string) error {
	return mw.WriteField(name, value)
}

func scriptWriteFile(mw *multipart.Writer, name, filename, contentType, content string) error {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", contentDisposition(name, filename))
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	pw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	_, err = io.WriteString(pw, content)
	return err
}

func scriptWriteRepeatingFile(mw *multipart.Writer, name, filename string, size int, fill byte) error {
	pw, err := mw.CreateFormFile(name, filename)
	if err != nil {
		return err
	}
	buf := make([]byte, 4096)
	for i := range buf {
		buf[i] = fill
	}
	remaining := size
	for remaining > 0 {
		n := len(buf)
		if n > remaining {
			n = remaining
		}
		if _, err := pw.Write(buf[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	return nil
}

func contentDisposition(name, filename string) string {
	var b strings.Builder
	b.WriteString(`form-data; name="`)
	b.WriteString(name)
	b.WriteString(`"`)
	if filename != "" {
		b.WriteString(`; filename="`)
		b.WriteString(filename)
		b.WriteString(`"`)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Goroutine fallback gate
// ---------------------------------------------------------------------------

func countConformanceGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	count := 0
	for _, st := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(st, "mazrean/formstream") || strings.Contains(st, "conformance_test") {
			count++
		}
	}
	return count
}

// assertGoroutinesFallback waits until the count of formstream-related
// goroutines returns to the baseline measured before the parse started. A
// production goroutine that fails to exit on reader failure, consumer stop or
// cancellation keeps the count above baseline and fails this gate.
func assertGoroutinesFallback(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last int
	for {
		last = countConformanceGoroutines()
		if last <= baseline {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutine fallback gate: %d conformance goroutines still alive (baseline %d)\n%s",
				last, baseline, string(buf[:n]))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// blockingReadCloser blocks Read until released (Close/cancel), used to pin
// the parser on input so context cancellation can be exercised deterministically.
type blockingReadCloser struct {
	gate       chan struct{}
	readCalls  atomic.Int64
	closeCalls atomic.Int64
	once       sync.Once
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{gate: make(chan struct{})}
}

func (b *blockingReadCloser) Read(p []byte) (int, error) {
	b.readCalls.Add(1)
	<-b.gate
	return 0, errFixtureReader
}

func (b *blockingReadCloser) Close() error {
	b.closeCalls.Add(1)
	b.once.Do(func() { close(b.gate) })
	return nil
}


// sortStrings sorts a slice in place (kept local to avoid a new helper file).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func errorIs(err, target error) bool { return errors.Is(err, target) }
