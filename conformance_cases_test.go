package formstream_test

// Fixed counterexample/corner-case inputs for
// "formstream: cross-adapter streaming consistency and leakage gate".
//
// Each contract runs through every public adapter and reports the first
// violated invariant with enough context to locate the breach.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

// largeFileSize is big enough to prove the body is never fully buffered (it
// far exceeds the 32KiB pipe-copy budget and any fixture chunk), but stays
// cheap enough to run under -race on every CI invocation.
const largeFileSize = 4 << 20 // 4 MiB

func forEachAdapter(t *testing.T, fn func(t *testing.T, a conformanceAdapter)) {
	t.Helper()
	for _, a := range adapters(t) {
		a := a
		t.Run(a.name, func(t *testing.T) {
			fn(t, a)
		})
	}
}

// parseResult records what the caller observed and what internal-facing
// invariants can be re-checked through the public surface.
type parseResult struct {
	err    error
	lookup func(string) (string, formstream.Header, bool)
}

// runParse drives one public adapter and installs the goroutine fallback
// gate around it. setup registers hooks; opts pass parser options.
func runParse(t *testing.T, a conformanceAdapter, ctx context.Context, form io.ReadCloser,
	opts []formstream.ParserOption, setup func(*registerHandle)) parseResult {
	t.Helper()
	baseline := countConformanceGoroutines()

	var res parseResult
	res.err = a.parse(ctx, form, opts, func(h *registerHandle) {
		setup(h)
		res.lookup = h.lookup
	})

	assertGoroutinesFallback(t, baseline)
	return res
}

func (r parseResult) value(t *testing.T, key string) string {
	t.Helper()
	v, _, ok := r.lookup(key)
	if !ok {
		t.Fatalf("expected non-hook field %q to be present after parse", key)
	}
	return v
}

// TestFormstreamConformance is the single entry of the regression material;
// subtest names identify the breached contract on failure.
func TestFormstreamConformance(t *testing.T) {
	t.Run("empty_part", testConformanceEmptyPart)
	t.Run("oversized_header", testConformanceOversizedHeader)
	t.Run("unicode_filename", testConformanceUnicodeFilename)
	t.Run("multiple_files_same_field", testConformanceMultipleFilesSameField)
	t.Run("slow_consumer", testConformanceSlowConsumer)
	t.Run("reader_midstream_failure", testConformanceReaderMidstreamFailure)
	t.Run("consumer_reads_prefix_only", testConformanceConsumerReadsPrefixOnly)
	t.Run("context_cancellation", testConformanceContextCancellation)
	t.Run("corrupt_input_error_layering", testConformanceCorruptInput)
	t.Run("resource_budget_exhausted", testConformanceBudgetExhausted)
}

// 1. empty part: zero-length field and zero-length file.
func testConformanceEmptyPart(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		form := newStreamForm()
		form.produce(func(mw *multipart.Writer) error {
			if err := scriptWriteField(mw, "emptyfield", ""); err != nil {
				return err
			}
			return scriptWriteFile(mw, "emptyfile", "empty.bin", "application/octet-stream", "")
		})

		var fileCalled atomic.Bool
		var fileSize int64 = -1
		res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
			if err := h.register("emptyfile", func(r io.Reader, _ formstream.Header) error {
				fileCalled.Store(true)
				n, err := io.Copy(io.Discard, r)
				fileSize = n
				return err
			}); err != nil {
				t.Fatalf("register emptyfile: %v", err)
			}
		})
		if res.err != nil {
			t.Fatalf("empty part: parse returned %v", res.err)
		}
		form.waitProducer(t)

		if !fileCalled.Load() {
			t.Fatal("empty part: file hook was never invoked")
		}
		if fileSize != 0 {
			t.Fatalf("empty part: expected 0 file bytes, got %d", fileSize)
		}
		if v := res.value(t, "emptyfield"); v != "" {
			t.Fatalf("empty part: expected empty field, got %q", v)
		}
	})
}

// 2. oversized header: a header line larger than the parser's read limit
// must be rejected through multipart's error channel, identically on every
// adapter, without goroutine leakage.
func testConformanceOversizedHeader(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		// mime/multipart rejects header sections larger than 10MiB with
		// "multipart: message too large". Emit the raw bytes (no Writer).
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "--%s\r\n", conformanceBoundary)
		fmt.Fprintf(&buf, "Content-Disposition: form-data; name=\"big\"\r\n")
		fmt.Fprintf(&buf, "X-Huge: %s\r\n", strings.Repeat("h", 11<<20))
		fmt.Fprintf(&buf, "\r\nvalue\r\n--%s--\r\n", conformanceBoundary)
		body := io.NopCloser(bytes.NewReader(buf.Bytes()))

		var hookCalled atomic.Bool
		res := runParse(t, a, context.Background(), body, nil, func(h *registerHandle) {
			if err := h.register("big", func(io.Reader, formstream.Header) error {
				hookCalled.Store(true)
				return nil
			}); err != nil {
				t.Fatalf("register big: %v", err)
			}
		})
		if res.err == nil {
			t.Fatal("oversized header: expected rejection error, got nil")
		}
		if hookCalled.Load() {
			t.Fatal("oversized header: hook must not run when its header section is invalid")
		}
		if !strings.Contains(res.err.Error(), "multipart") {
			t.Fatalf("oversized header: error should come from the multipart layer, got %v", res.err)
		}
	})
}

// 3. Unicode filename: RFC 5987/6532-encoded filename round-trips.
func testConformanceUnicodeFilename(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		const unicodeName = "テスト-日本語-🚀.txt"
		form := newStreamForm()
		form.produce(func(mw *multipart.Writer) error {
			return scriptWriteFile(mw, "doc", unicodeName, "text/plain; charset=utf-8", "payload-\u30c6\u30b9\u30c8")
		})

		var gotName, gotType, gotContent string
		res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
			if err := h.register("doc", func(r io.Reader, hdr formstream.Header) error {
				gotName = hdr.FileName()
				gotType = hdr.ContentType()
				b, err := io.ReadAll(r)
				gotContent = string(b)
				return err
			}); err != nil {
				t.Fatalf("register doc: %v", err)
			}
		})
		if res.err != nil {
			t.Fatalf("unicode filename: parse returned %v", res.err)
		}
		form.waitProducer(t)

		if gotName != unicodeName {
			t.Fatalf("unicode filename: expected %q, got %q", unicodeName, gotName)
		}
		if !strings.HasPrefix(gotType, "text/plain") {
			t.Fatalf("unicode filename: expected text/plain content type, got %q", gotType)
		}
		if gotContent != "payload-テスト" {
			t.Fatalf("unicode filename: payload mismatch: %q", gotContent)
		}
	})
}

// 4. multiple files under the same field: every occurrence reaches the hook
// with its own header and complete contents.
func testConformanceMultipleFilesSameField(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		names := []string{"a.txt", "b.txt", "c.txt"}
		contents := []string{"aaa", strings.Repeat("b", 70000), "ccc"}
		form := newStreamForm()
		form.produce(func(mw *multipart.Writer) error {
			if err := scriptWriteField(mw, "meta", "m"); err != nil {
				return err
			}
			for i := range names {
				if err := scriptWriteFile(mw, "uploads", names[i], "text/plain", contents[i]); err != nil {
					return err
				}
			}
			return nil
		})

		type seen struct {
			name    string
			content string
		}
		var mu struct {
			files []seen
		}
		var count atomic.Int32
		res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
			if err := h.register("uploads", func(r io.Reader, hdr formstream.Header) error {
				b, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				mu.files = append(mu.files, seen{name: hdr.FileName(), content: string(b)})
				count.Add(1)
				return nil
			}, formstream.WithRequiredPart("meta")); err != nil {
				t.Fatalf("register uploads: %v", err)
			}
		})
		if res.err != nil {
			t.Fatalf("multiple files: parse returned %v", res.err)
		}
		form.waitProducer(t)

		if count.Load() != 3 {
			t.Fatalf("multiple files: expected 3 hook calls, got %d", count.Load())
		}
		if v := res.value(t, "meta"); v != "m" {
			t.Fatalf("multiple files: meta field mismatch %q", v)
		}
		for i, want := range names {
			found := false
			for _, f := range mu.files {
				if f.name == want && f.content == contents[i] {
					found = true
				}
			}
			if !found {
				t.Fatalf("multiple files: file %q with %d bytes missing from hook observations: %+v",
					want, len(contents[i]), mu.files)
			}
		}
	})
}

// 5. slow consumer: a hook that drains slowly must not force the whole file
// into memory (the largest Read handed to the parser stays at the 32KiB
// pipe-copy granularity), and the form producer exits on completion.
func testConformanceSlowConsumer(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		form := newStreamForm()
		form.produce(func(mw *multipart.Writer) error {
			return scriptWriteRepeatingFile(mw, "blob", "blob.bin", largeFileSize, 'z')
		})

		var got int64
		res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
			if err := h.register("blob", func(r io.Reader, _ formstream.Header) error {
				buf := make([]byte, 256)
				for {
					n, err := r.Read(buf)
					got += int64(n)
					if errors.Is(err, io.EOF) {
						return nil
					}
					if err != nil {
						return err
					}
					time.Sleep(50 * time.Microsecond) // deliberately slow consumer
				}
			}); err != nil {
				t.Fatalf("register blob: %v", err)
			}
		})
		if res.err != nil {
			t.Fatalf("slow consumer: parse returned %v", res.err)
		}
		form.waitProducer(t)

		if got != largeFileSize {
			t.Fatalf("slow consumer: consumed %d of %d bytes", got, largeFileSize)
		}
		// The pipe copies in <=32KiB pieces; a broken implementation that
		// buffers the part before invoking the hook reports >= largeFileSize.
		if max := form.maxRead.Load(); max > 32*1024 {
			t.Fatalf("slow consumer: streaming gate breached, largest Read = %d bytes (>32KiB)", max)
		}
		if read := form.bytesRead.Load(); read < largeFileSize {
			t.Fatalf("slow consumer: only %d of %d body bytes streamed", read, largeFileSize)
		}
	})
}

// 6. reader mid-stream failure: the producer must surface the injected
// causal error (not io.ErrUnexpectedEOF or a framing error), the hook sees
// the error through its reader, all goroutines exit, and the source is
// closed. A subsequent retry with identical, intact input succeeds.
func testConformanceReaderMidstreamFailure(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		form := failingMidstreamForm(t, largeFileSize)

		var hookSawError atomic.Bool
		var hookRead int64
		res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
			if err := h.register("blob", func(r io.Reader, _ formstream.Header) error {
				buf := make([]byte, 4096)
				for {
					n, err := r.Read(buf)
					hookRead += int64(n)
					if err != nil {
						if errors.Is(err, errFixtureReader) {
							hookSawError.Store(true)
						}
						return nil // the hook observed the failure but keeps it as evidence
					}
				}
			}); err != nil {
				t.Fatalf("register blob: %v", err)
			}
		})

		if res.err == nil {
			t.Fatal("reader failure: expected parse to fail, got nil")
		}
		if !errors.Is(res.err, errFixtureReader) {
			t.Fatalf("reader failure: first causal error lost: errors.Is(%v, errFixtureReader)=false", res.err)
		}
		form.waitProducer(t)
		if closes := form.closeCalls.Load(); closes == 0 {
			t.Fatal("reader failure: production path never closed the failing source")
		}
		if !hookSawError.Load() {
			t.Fatal("reader failure: hook reader did not expose the injected error")
		}
		if hookRead >= largeFileSize {
			t.Fatalf("reader failure: hook consumed %d bytes despite mid-stream failure at %d",
				hookRead, largeFileSize/2)
		}

		// Recoverable retry: identical input, brand new parser, stable success.
		good := newStreamForm()
		good.produce(func(mw *multipart.Writer) error {
			return scriptWriteRepeatingFile(mw, "blob", "blob.bin", largeFileSize, 'z')
		})
		var retryGot int64
		retry := runParse(t, a, context.Background(), good, nil, func(h *registerHandle) {
			if err := h.register("blob", func(r io.Reader, _ formstream.Header) error {
				n, err := io.Copy(io.Discard, r)
				retryGot = n
				return err
			}); err != nil {
				t.Fatalf("register blob (retry): %v", err)
			}
		})
		if retry.err != nil {
			t.Fatalf("reader failure: retry after recovery returned %v", retry.err)
		}
		good.waitProducer(t)
		if retryGot != largeFileSize {
			t.Fatalf("reader failure: retry consumed %d of %d bytes", retryGot, largeFileSize)
		}
	})
}

// failingMidstreamForm builds a large file part whose producer aborts with
// errFixtureReader halfway, without writing a closing boundary.
func failingMidstreamForm(t *testing.T, size int) *streamForm {
	t.Helper()
	form := newStreamForm()
	form.produce(func(mw *multipart.Writer) error {
		pw, err := mw.CreateFormFile("blob", "blob.bin")
		if err != nil {
			return err
		}
		buf := make([]byte, 4096)
		for i := range buf {
			buf[i] = 'z'
		}
		written := 0
		for written < size {
			n := len(buf)
			if written+n > size/2 {
				n = size/2 - written
			}
			if n <= 0 {
				break
			}
			if _, err := pw.Write(buf[:n]); err != nil {
				return err
			}
			written += n
			if written >= size/2 {
				return errFixtureReader // recoverable, mid-stream failure
			}
		}
		return nil
	})
	return form
}

// 6b. Consumer reads only a prefix: the hook returns early (with or without
// an error). The parser must still terminate cleanly, drain/release the part,
// close the source and not leave any goroutine behind. A hook error is the
// first causal error.
func testConformanceConsumerReadsPrefixOnly(t *testing.T) {
	for _, hookErr := range []error{nil, errConsumerAborted} {
		hookErr := hookErr
		label := "early-return"
		if hookErr != nil {
			label = "early-error"
		}
		t.Run(label, func(t *testing.T) {
			forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
				form := newStreamForm()
				form.produce(func(mw *multipart.Writer) error {
					return scriptWriteRepeatingFile(mw, "blob", "blob.bin", largeFileSize, 'q')
				})

				res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
					if err := h.register("blob", func(r io.Reader, _ formstream.Header) error {
						p := make([]byte, 128)
						if _, err := io.ReadFull(r, p); err != nil {
							return err
						}
						return hookErr // stop consuming after a 128-byte prefix
					}); err != nil {
						t.Fatalf("register blob: %v", err)
					}
				})

				switch {
				case hookErr == nil && res.err != nil:
					t.Fatalf("prefix-only: early return must succeed, got %v", res.err)
				case hookErr != nil && !errors.Is(res.err, errConsumerAborted):
					t.Fatalf("prefix-only: first causal error lost, want %v got %v", errConsumerAborted, res.err)
				}
				form.waitProducer(t)
				if hookErr != nil && form.closeCalls.Load() == 0 {
					t.Fatal("prefix-only: source was not closed when the consumer aborted with an error")
				}
				if hookErr == nil && form.bytesRead.Load() < largeFileSize {
					t.Fatalf("prefix-only: remaining part was not drained (%d/%d bytes)",
						form.bytesRead.Load(), largeFileSize)
				}
			})
		})
	}
}

// 7. context cancellation while blocked on input: ParseContext returns the
// context cause and every production goroutine exits.
func testConformanceContextCancellation(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		src := newBlockingReadCloser()
		ctx, cancel := context.WithCancel(context.Background())
		baseline := countConformanceGoroutines()

		done := make(chan error, 1)
		go func() {
			done <- a.parse(ctx, src, nil, func(h *registerHandle) {
				if err := h.register("blob", func(io.Reader, formstream.Header) error { return nil }); err != nil {
					t.Errorf("register blob: %v", err)
				}
			})
		}()

		// Wait until the parser is parked on the blocked source read.
		deadline := time.Now().Add(2 * time.Second)
		for src.readCalls.Load() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("context cancel: parser never reached the blocking read")
			}
			time.Sleep(5 * time.Millisecond)
		}

		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("context cancel: expected context.Canceled, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("context cancel: ParseContext did not return after cancellation")
		}
		assertGoroutinesFallback(t, baseline)
		if closes := src.closeCalls.Load(); closes == 0 {
			t.Fatal("context cancel: blocked source was not closed")
		}
	})
}

// 8. corrupt input: malformed framing must surface the multipart layer error
// (stable across adapters and repeats).
func testConformanceCorruptInput(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		raw := "--" + conformanceBoundary + "\r\n" +
			"Content-Disposition: form-data; name=\"x\"\r\n\r\n" +
			"v\r\n--" + conformanceBoundary + "--JUNK-TRAILING-BYTES\r\n"
		body := io.NopCloser(strings.NewReader(raw))
		res := runParse(t, a, context.Background(), body, nil, func(h *registerHandle) {
			if err := h.register("x", func(io.Reader, formstream.Header) error { return nil }); err != nil {
				t.Fatalf("register x: %v", err)
			}
		})
		if res.err == nil {
			t.Fatal("corrupt input: expected an error, got nil")
		}
		if !strings.Contains(res.err.Error(), "multipart") {
			t.Fatalf("corrupt input: expected multipart-layer error, got %v", res.err)
		}
	})
}

// 9. resource budget exhausted: exceeding MaxMemSize on a non-hook field
// returns ErrTooLargeForm on every adapter.
func testConformanceBudgetExhausted(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		form := newStreamForm()
		form.produce(func(mw *multipart.Writer) error {
			return scriptWriteField(mw, "big", strings.Repeat("y", 4096))
		})
		opts := []formstream.ParserOption{formstream.WithMaxMemSize(1024)}
		res := runParse(t, a, context.Background(), form, opts, func(h *registerHandle) {
			if err := h.register("other", func(io.Reader, formstream.Header) error { return nil }); err != nil {
				t.Fatalf("register other: %v", err)
			}
		})
		if !errors.Is(res.err, formstream.ErrTooLargeForm) {
			t.Fatalf("budget: expected ErrTooLargeForm, got %v", res.err)
		}
		form.waitProducer(t)
	})
}
