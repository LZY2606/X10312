// Package conformance_test holds the regression material for
// "formstream: cross-adapter streaming consistency and leak gates".
//
// Every case enters through a public entry point (the core
// formstream.Parser or the net/http, gin, echo adapters) and only observes
// public results plus externally visible invariants (source reads, temp
// files, goroutine count). No internal final objects are fabricated.
package conformance_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/labstack/echo/v4"
	"github.com/mazrean/formstream"
	echoform "github.com/mazrean/formstream/echo"
	ginform "github.com/mazrean/formstream/gin"
	httpform "github.com/mazrean/formstream/http"
)

// parseTimeout bounds every producer goroutine; a parse that does not
// return within it is reported as a leaked producer.
const parseTimeout = 15 * time.Second

// parserHandle is the uniform view the harness drives: registration,
// parsing and value lookup all go through the public API of one adapter.
type parserHandle struct {
	register func(name string, fn formstream.StreamHookFunc, opts ...formstream.RegisterOption) error
	parse    func(ctx context.Context) error
	value    func(key string) (string, formstream.Header, bool)
	values   func(key string) ([]string, bool)
}

func handleOf(p *formstream.Parser, parse func(ctx context.Context) error) *parserHandle {
	return &parserHandle{
		register: func(name string, fn formstream.StreamHookFunc, opts ...formstream.RegisterOption) error {
			return p.Register(name, fn, opts...)
		},
		parse: parse,
		value: func(key string) (string, formstream.Header, bool) {
			return p.Value(key)
		},
		values: func(key string) ([]string, bool) {
			vs, ok := p.Values(key)
			if !ok {
				return nil, false
			}
			out := make([]string, 0, len(vs))
			for _, v := range vs {
				s, _ := v.Unwrap()
				out = append(out, s)
			}
			return out, true
		},
	}
}

// adapter is one public entry point under test.
type adapter struct {
	name      string
	newHandle func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error)
}

func allAdapters() []adapter {
	newRequest := func(body io.Reader, boundary string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/upload", body)
		req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
		return req
	}

	return []adapter{
		{
			name: "core",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				p := formstream.NewParser(boundary, opts...)
				return handleOf(p, func(ctx context.Context) error {
					return p.ParseContext(ctx, body)
				}), nil
			},
		},
		{
			name: "net/http",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				p, err := httpform.NewParser(newRequest(body, boundary), opts...)
				if err != nil {
					return nil, err
				}
				return handleOf(p.Parser, p.ParseContext), nil
			},
		},
		{
			name: "gin",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = newRequest(body, boundary)
				p, err := ginform.NewParser(c, opts...)
				if err != nil {
					return nil, err
				}
				return handleOf(p.Parser, p.ParseContext), nil
			},
		},
		{
			name: "echo",
			newHandle: func(body io.Reader, boundary string, opts ...formstream.ParserOption) (*parserHandle, error) {
				c := echo.New().NewContext(newRequest(body, boundary), httptest.NewRecorder())
				p, err := echoform.NewParser(c, opts...)
				if err != nil {
					return nil, err
				}
				return handleOf(p.Parser, p.ParseContext), nil
			},
		},
	}
}

// forEachAdapter runs fn once per public adapter so a failure name points
// at both the contract and the entry point that violated it.
func forEachAdapter(t *testing.T, fn func(t *testing.T, ad adapter)) {
	t.Helper()
	for _, ad := range allAdapters() {
		ad := ad
		t.Run(ad.name, func(t *testing.T) {
			fn(t, ad)
		})
	}
}

// partSpec describes one part of a fixture form.
type partSpec struct {
	name         string
	filename     string // quoted as-is into Content-Disposition
	filenameStar string // RFC 2231 filename* parameter (already encoded)
	contentType  string
	headers      map[string]string
	content      []byte
}

// buildForm renders a fixture form through mime/multipart, the same public
// encoder a real client would use.
func buildForm(t *testing.T, boundary string, parts ...partSpec) []byte {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	mw := multipart.NewWriter(buf)
	if err := mw.SetBoundary(boundary); err != nil {
		t.Fatalf("failed to set boundary: %s", err)
	}

	quote := func(s string) string {
		return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
	}

	for _, spec := range parts {
		isPlainField := spec.filename == "" && spec.filenameStar == "" && spec.contentType == "" && spec.headers == nil
		if isPlainField {
			if err := mw.WriteField(spec.name, string(spec.content)); err != nil {
				t.Fatalf("failed to write field %q: %s", spec.name, err)
			}
			continue
		}

		h := textproto.MIMEHeader{}
		cd := fmt.Sprintf(`form-data; name=%s`, quote(spec.name))
		if spec.filename != "" {
			cd += fmt.Sprintf(`; filename=%s`, quote(spec.filename))
		}
		if spec.filenameStar != "" {
			cd += `; filename*=` + spec.filenameStar
		}
		h.Set("Content-Disposition", cd)
		ct := spec.contentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		h.Set("Content-Type", ct)
		for k, v := range spec.headers {
			h.Set(k, v)
		}

		pw, err := mw.CreatePart(h)
		if err != nil {
			t.Fatalf("failed to create part %q: %s", spec.name, err)
		}
		if _, err := pw.Write(spec.content); err != nil {
			t.Fatalf("failed to write part %q: %s", spec.name, err)
		}
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %s", err)
	}

	return buf.Bytes()
}

// statReader wraps the source body and records how the parser pulls from
// it: read/close counts, total bytes and the largest single read chunk
// (the peak buffer the implementation ever requested from the source).
type statReader struct {
	r       io.Reader
	failAt  int64 // fail on the first read at/after this offset; <0 disables
	failErr error

	mu       sync.Mutex
	reads    int
	closes   int
	bytes    int64
	maxChunk int
}

func newStatReader(r io.Reader) *statReader {
	return &statReader{r: r, failAt: -1}
}

func (s *statReader) Read(p []byte) (int, error) {
	s.mu.Lock()
	if s.failAt >= 0 && s.bytes >= s.failAt {
		err := s.failErr
		s.mu.Unlock()
		return 0, err
	}
	s.mu.Unlock()

	n, err := s.r.Read(p)

	s.mu.Lock()
	s.reads++
	s.bytes += int64(n)
	if n > s.maxChunk {
		s.maxChunk = n
	}
	s.mu.Unlock()

	return n, err
}

func (s *statReader) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

type readStats struct {
	reads    int
	closes   int
	bytes    int64
	maxChunk int
}

func (s *statReader) stats() readStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return readStats{reads: s.reads, closes: s.closes, bytes: s.bytes, maxChunk: s.maxChunk}
}

// trickleReader serves a valid multipart prefix and then an endless slow
// stream of 'a's, so a parse only terminates via cancellation or error.
type trickleReader struct {
	prefix []byte
	off    int
	delay  time.Duration
}

func (tr *trickleReader) Read(p []byte) (int, error) {
	if tr.off < len(tr.prefix) {
		n := copy(p, tr.prefix[tr.off:])
		tr.off += n
		return n, nil
	}
	time.Sleep(tr.delay)
	p[0] = 'a'
	return 1, nil
}

// runParse runs one parse in a producer goroutine and fails the test if
// the goroutine does not report back within parseTimeout.
func runParse(t *testing.T, h *parserHandle, ctx context.Context) error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- h.parse(ctx)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(parseTimeout):
		t.Fatalf("producer goroutine did not exit within %s", parseTimeout)
		return nil
	}
}

// leakGuard snapshots goroutine count and formstream temp files so a test
// can prove resources were released afterwards.
type leakGuard struct {
	t          *testing.T
	goroutines int
	tempFiles  map[string]struct{}
}

func newLeakGuard(t *testing.T) *leakGuard {
	t.Helper()
	return &leakGuard{
		t:          t,
		goroutines: runtime.NumGoroutine(),
		tempFiles:  formstreamTempFiles(t),
	}
}

func formstreamTempFiles(t *testing.T) map[string]struct{} {
	t.Helper()
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("failed to read temp dir: %s", err)
	}
	set := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "formstream-") {
			set[e.Name()] = struct{}{}
		}
	}
	return set
}

// settle asserts the goroutine count falls back to the snapshot and no new
// formstream temp file survived, i.e. producers exited and spilled files
// were closed and removed.
func (g *leakGuard) settle() {
	g.t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= g.goroutines {
			break
		} else if time.Now().After(deadline) {
			g.t.Fatalf("goroutine leak gate: before=%d, now=%d", g.goroutines, n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	for name := range formstreamTempFiles(g.t) {
		if _, ok := g.tempFiles[name]; !ok {
			g.t.Fatalf("temp file %q was not released", name)
		}
	}
}
