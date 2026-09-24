package formstream

// White-box resource accounting tests for the streaming production path.
// These tests still enter through the public API (NewParser/ParseContext);
// they only *observe* unexported parser/preProcessor state afterwards, which
// the external conformance suite cannot reach. The observation hooks are
// internal because they inspect implementation details; no production
// algorithm is duplicated for tests.

import (
	"context"
	"errors"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// buildBody assembles a multipart form in-memory (forms here are small).
func buildBody(t *testing.T, boundary string, parts ...func(*multipart.Writer) error) io.ReadCloser {
	t.Helper()
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	if err := mw.SetBoundary(boundary); err != nil {
		t.Fatal(err)
	}
	go func() {
		for _, p := range parts {
			if err := p(mw); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
		_ = pw.CloseWithError(mw.Close())
	}()
	return pr
}

func field(name, value string) func(*multipart.Writer) error {
	return func(mw *multipart.Writer) error { return mw.WriteField(name, value) }
}

func fileSmall(name, filename string, n int) func(*multipart.Writer) error {
	return func(mw *multipart.Writer) error {
		pw, err := mw.CreateFormFile(name, filename)
		if err != nil {
			return err
		}
		_, err = io.Copy(pw, strings.NewReader(strings.Repeat("s", n)))
		return err
	}
}

// Successful parse that pre-processed (deferred) parts restores the memory
// budget and returns pooled buffers; the temp file is removed by Close.
func TestResourceAccounting_DeferredPartBudgetRestored(t *testing.T) {
	t.Parallel()

	const budget DataSize = 64
	p := NewParser("b",
		WithMaxMemSize(budget),
		WithMaxMemFileSize(budget),
	)
	if err := p.Register("later", func(r io.Reader, _ Header) error {
		// fully consumes the deferred, spooled content
		n, err := io.Copy(io.Discard, r)
		if err != nil {
			return err
		}
		if n != int64(budget)+10 {
			t.Fatalf("deferred content size: want %d got %d", int64(budget)+10, n)
		}
		return nil
	}, WithRequiredPart("meta")); err != nil {
		t.Fatal(err)
	}

	body := buildBody(t, "b",
		fileSmall("later", "f.bin", int(budget)+10), // larger than budget -> disk
		field("meta", "m"),
	)
	if err := p.ParseContext(context.Background(), body); err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got := p.maxMemSize; got != budget {
		t.Fatalf("maxMemSize not restored: want %d got %d", budget, got)
	}
	if got := p.maxMemFileSize; got != budget {
		t.Fatalf("maxMemFileSize not restored: want %d got %d", budget, got)
	}
}

// A failed parse before a deferred hook becomes satisfiable releases every
// pre-processed part (no pooled buffer / temp section survives).
func TestResourceAccounting_FailedParseReleasesPendingParts(t *testing.T) {
	t.Parallel()

	const budget DataSize = 64
	p := NewParser("b",
		WithMaxMemSize(budget),
		WithMaxMemFileSize(budget),
	)
	hookRan := false
	if err := p.Register("later", func(io.Reader, Header) error {
		hookRan = true
		return nil
	}, WithRequiredPart("never-arrives")); err != nil {
		t.Fatal(err)
	}

	// one memory-backed and one disk-backed deferred part, then reader fails.
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	if err := mw.SetBoundary("b"); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = writePart(mw, "later", "small.txt", 10)
		_ = writePart(mw, "later", "big.txt", int(budget)+20)
		_ = pw.CloseWithError(errors.New("boom: mid-stream reader failure"))
	}()

	err := p.ParseContext(context.Background(), pr)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected wrapped reader error, got %v", err)
	}
	if hookRan {
		t.Fatal("deferred hook must not run when its requirement never arrives")
	}

	if got := p.maxMemSize; got != budget {
		t.Fatalf("maxMemSize leaked after failed parse: want %d got %d", budget, got)
	}
	if got := p.maxMemFileSize; got != budget {
		t.Fatalf("maxMemFileSize leaked after failed parse: want %d got %d", budget, got)
	}
}

// Temp files created for over-budget parts are always unlinked.
func TestResourceAccounting_TempFileRemoved(t *testing.T) {
	// not parallel: overrides TMPDIR for the process while running.
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	run := func(fail bool) {
		p := NewParser("b", WithMaxMemSize(32), WithMaxMemFileSize(32))
		if err := p.Register("f", func(r io.Reader, _ Header) error {
			if fail {
				return errConsumerTest
			}
			_, err := io.Copy(io.Discard, r)
			return err
		}, WithRequiredPart("meta")); err != nil {
			t.Fatal(err)
		}
		if fail {
			pr, pw := io.Pipe()
			mw := multipart.NewWriter(pw)
			_ = mw.SetBoundary("b")
			go func() {
				_ = writePart(mw, "f", "f.bin", 200)
				_ = pw.CloseWithError(errors.New("boom"))
			}()
			_ = p.ParseContext(context.Background(), pr)
		} else {
			body := buildBody(t, "b", fileSmall("f", "f.bin", 200), field("meta", "m"))
			if err := p.ParseContext(context.Background(), body); err != nil {
				t.Fatal(err)
			}
		}
	}

	run(false)
	run(true)

	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "formstream-") {
			t.Fatalf("temporary file leaked: %s", filepath.Join(tmp, e.Name()))
		}
	}
}

var errConsumerTest = errors.New("resource accounting: consumer error")

func writePart(mw *multipart.Writer, name, filename string, n int) error {
	pw, err := mw.CreateFormFile(name, filename)
	if err != nil {
		return err
	}
	buf := make([]byte, 4096)
	remaining := n
	for remaining > 0 {
		m := len(buf)
		if m > remaining {
			m = remaining
		}
		if _, err := pw.Write(buf[:m]); err != nil {
			return err
		}
		remaining -= m
	}
	return nil
}
