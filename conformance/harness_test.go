// Package conformance_test is the regression harness for
// "formstream: cross-adapter streaming consistency and leak gates".
//
// Every scenario enters through the repository's public entry points
// (formstream.NewParser/Register/Parse and the http/echo/gin adapters).
// No internal final objects are fabricated: the harness only wraps
// io.Reader inputs and observes the public outputs (values, hook
// callbacks, errors, temp files, goroutine count). The instrumented
// readers below share the exact production parse path; they only
// observe/inject at the io.Reader boundary.
package conformance_test

import (
	"bytes"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/labstack/echo/v4"
	"github.com/mazrean/formstream"
	echoform "github.com/mazrean/formstream/echo"
	ginform "github.com/mazrean/formstream/gin"
	httpform "github.com/mazrean/formstream/http"
)

// parserHandle exposes the uniform public surface shared by the core
// parser and every adapter, so one scenario runs unchanged on all of
// them.
type parserHandle struct {
	register func(name string, fn formstream.StreamHookFunc, opts ...formstream.RegisterOption) error
	parse    func() error
	value    func(key string) (string, formstream.Header, bool)
	values   func(key string) ([]formstream.Value, bool)
}

type adapter struct {
	name string
	new  func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error)
}

func handleOf(p *formstream.Parser, parse func() error) *parserHandle {
	return &parserHandle{
		register: p.Register,
		parse:    parse,
		value:    p.Value,
		values:   p.Values,
	}
}

func newRequest(body io.Reader, boundary string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	return req
}

// adapters lists every public entry point of the repository.
func adapters() []adapter {
	return []adapter{
		{
			name: "core",
			new: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				p := formstream.NewParser(boundary, opts...)
				return handleOf(p, func() error { return p.Parse(body) }), nil
			},
		},
		{
			name: "http",
			new: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				p, err := httpform.NewParser(newRequest(body, boundary), opts...)
				if err != nil {
					return nil, err
				}
				return handleOf(p.Parser, p.Parse), nil
			},
		},
		{
			name: "echo",
			new: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				e := echo.New()
				c := e.NewContext(newRequest(body, boundary), httptest.NewRecorder())
				p, err := echoform.NewParser(c, opts...)
				if err != nil {
					return nil, err
				}
				return handleOf(p.Parser, p.Parse), nil
			},
		},
		{
			name: "gin",
			new: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				gin.SetMode(gin.TestMode)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = newRequest(body, boundary)
				p, err := ginform.NewParser(c, opts...)
				if err != nil {
					return nil, err
				}
				return handleOf(p.Parser, p.Parse), nil
			},
		},
	}
}

// sourceReader is an instrumented input reader. It records read counts,
// total bytes and the peak single-read fill (peak buffer observed at the
// source), and can inject a recoverable failure once failAt bytes have
// been produced.
type sourceReader struct {
	r        io.Reader
	reads    int
	bytes    int64
	maxChunk int
	failAt   int64 // <0 disables failure injection
	failErr  func() error
}

func (s *sourceReader) Read(p []byte) (int, error) {
	if s.failAt >= 0 && s.bytes >= s.failAt {
		return 0, s.failErr()
	}
	n, err := s.r.Read(p)
	s.reads++
	s.bytes += int64(n)
	if n > s.maxChunk {
		s.maxChunk = n
	}
	return n, err
}

// hookSpy instruments the consumer side of a stream hook: it records
// call/read/close counts, captured headers and contents, the number of
// source bytes already consumed when the hook was entered (streaming
// evidence), and whether a formstream temp file existed while the hook
// ran (disk-spill evidence).
type hookSpy struct {
	calls            int
	reads            int
	bytes            int64
	closes           int
	headers          []formstream.Header
	contents         [][]byte
	sourceAtEntry    []int64
	tempFilesAtEntry []int

	src       *sourceReader // may be nil
	tempDir   string        // may be empty
	readLimit int64         // >=0: consumer reads only this prefix
	slow      bool          // consumer copies through myio slow writer
	err       error         // error returned by the consumer
}

type spyReadCloser struct {
	r   io.Reader
	c   io.Closer
	spy *hookSpy
}

func (s *spyReadCloser) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.spy.reads++
	s.spy.bytes += int64(n)
	return n, err
}

func (s *spyReadCloser) Close() error {
	s.spy.closes++
	return s.c.Close()
}

func (s *hookSpy) fn() formstream.StreamHookFunc {
	return func(r io.Reader, h formstream.Header) error {
		s.calls++
		s.headers = append(s.headers, h)
		if s.src != nil {
			s.sourceAtEntry = append(s.sourceAtEntry, s.src.bytes)
		}
		if s.tempDir != "" {
			s.tempFilesAtEntry = append(s.tempFilesAtEntry, len(formstreamTempFiles(s.tempDir)))
		}

		var rc io.Reader = &spyReadCloser{r: r, spy: s}
		if c, ok := r.(io.Closer); ok {
			rc = &spyReadCloser{r: r, c: c, spy: s}
		}

		var content []byte
		var err error
		switch {
		case s.readLimit >= 0:
			content, err = readAtMost(rc, s.readLimit)
		case s.slow:
			content, err = readAllSlow(rc)
		default:
			content, err = io.ReadAll(rc)
		}
		if err != nil {
			return fmt.Errorf("spy consumer: %w", err)
		}
		s.contents = append(s.contents, content)

		return s.err
	}
}

func readAtMost(r io.Reader, limit int64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	_, err := io.CopyN(buf, r, limit)
	if err != nil && err != io.EOF {
		return buf.Bytes(), err
	}
	return buf.Bytes(), nil
}

func readAllSlow(r io.Reader) ([]byte, error) {
	// myio.SlowWriter throttles per byte written; tee through it so the
	// consumer is slow while still capturing the content.
	buf := bytes.NewBuffer(nil)
	w := io.MultiWriter(buf, slowWriter{})
	_, err := io.Copy(w, r)
	return buf.Bytes(), err
}

// slowWriter mirrors internal/myio.SlowWriter without depending on the
// internal package from an external test package.
type slowWriter struct{}

func (slowWriter) Write(p []byte) (int, error) {
	time.Sleep(time.Duration(len(p)) * 20 * time.Nanosecond)
	return len(p), nil
}

// partSpec describes one part of a multipart body built through the real
// mime/multipart encoder.
type partSpec struct {
	name           string
	filename       string // encoded via mime.FormatMediaType (RFC 5987 when non-ASCII)
	rawDisposition string // verbatim Content-Disposition, overrides name/filename
	contentType    string
	extraHeaders   [][2]string
	content        []byte
}

func buildBody(t *testing.T, boundary string, parts ...partSpec) []byte {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	mw := multipart.NewWriter(buf)
	if err := mw.SetBoundary(boundary); err != nil {
		t.Fatalf("failed to set boundary: %s", err)
	}

	for _, p := range parts {
		disposition := p.rawDisposition
		if disposition == "" {
			params := map[string]string{"name": p.name}
			if p.filename != "" {
				params["filename"] = p.filename
			}
			disposition = mime.FormatMediaType("form-data", params)
		}

		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", disposition)
		if p.contentType != "" {
			header.Set("Content-Type", p.contentType)
		}
		for _, kv := range p.extraHeaders {
			header.Add(kv[0], kv[1])
		}

		pw, err := mw.CreatePart(header)
		if err != nil {
			t.Fatalf("failed to create part %q: %s", p.name, err)
		}
		if _, err := pw.Write(p.content); err != nil {
			t.Fatalf("failed to write part %q: %s", p.name, err)
		}
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %s", err)
	}

	return buf.Bytes()
}

// formstreamTempFiles lists temp files created by the parser's
// preProcessor (os.CreateTemp("", "formstream-")) inside dir.
func formstreamTempFiles(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "formstream-") {
			names = append(names, e.Name())
		}
	}
	return names
}

// isolateTempDir points TMPDIR at a test-private directory so temp-file
// observations are not polluted by other test processes, and returns the
// directory.
func isolateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	return dir
}

// goroutineGate snapshots the goroutine count and returns a check that
// fails the test unless the count falls back to the snapshot within a
// grace period.
func goroutineGate(t *testing.T) func() {
	t.Helper()
	before := runtime.NumGoroutine()
	return func() {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			after := runtime.NumGoroutine()
			if after <= before {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("goroutine leak gate violated: before=%d after=%d", before, after)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// mustRegister registers a hook and fails the test on error.
func mustRegister(t *testing.T, h *parserHandle, name string, fn formstream.StreamHookFunc, opts ...formstream.RegisterOption) {
	t.Helper()
	if err := h.register(name, fn, opts...); err != nil {
		t.Fatalf("failed to register hook %q: %s", name, err)
	}
}

// valueStrings unwraps all values of key in order.
func valueStrings(t *testing.T, h *parserHandle, key string) ([]string, bool) {
	t.Helper()
	values, ok := h.values(key)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		s, _ := v.Unwrap()
		out = append(out, s)
	}
	return out, true
}
