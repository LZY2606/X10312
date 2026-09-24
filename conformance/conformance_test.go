package conformance_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/myio"
)

// hookRecord captures what a stream hook observed, in call order.
type hookRecord struct {
	mu       sync.Mutex
	filename []string
	content  [][]byte
}

func (r *hookRecord) hook() formstream.StreamHookFunc {
	return func(pr io.Reader, header formstream.Header) error {
		b, err := io.ReadAll(pr)
		r.mu.Lock()
		r.filename = append(r.filename, header.FileName())
		r.content = append(r.content, b)
		r.mu.Unlock()
		return err
	}
}

func (r *hookRecord) snapshot() (filenames []string, contents [][]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.filename...), append([][]byte(nil), r.content...)
}

// TestConformanceEmptyPart covers empty parts: an empty value field must be
// observable as present-but-empty, and an empty file part must reach the
// hook with zero bytes, identically across adapters.
func TestConformanceEmptyPart(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, ad adapter) {
		guard := newLeakGuard(t)
		defer guard.settle()

		body := buildForm(t, "boundary",
			partSpec{name: "empty", content: nil},
			partSpec{name: "emptyfile", filename: "empty.txt", contentType: "text/plain", content: nil},
		)
		src := newStatReader(strings.NewReader(string(body)))

		h, err := ad.newHandle(src, "boundary")
		if err != nil {
			t.Fatalf("failed to create parser: %s", err)
		}
		rec := &hookRecord{}
		if err := h.register("emptyfile", rec.hook()); err != nil {
			t.Fatalf("failed to register hook: %s", err)
		}

		if err := runParse(t, h, context.Background()); err != nil {
			t.Fatalf("parse failed: %s", err)
		}

		v, _, ok := h.value("empty")
		if !ok {
			t.Fatal("empty field must be present in the value map")
		}
		if v != "" {
			t.Fatalf("empty field content: expected %q, got %q", "", v)
		}

		filenames, contents := rec.snapshot()
		if len(filenames) != 1 || filenames[0] != "empty.txt" {
			t.Fatalf("hook filenames: expected [empty.txt], got %v", filenames)
		}
		if len(contents) != 1 || len(contents[0]) != 0 {
			t.Fatalf("hook contents: expected one empty part, got %q", contents)
		}
		t.Logf("reads=%d bytes=%d peakChunk=%d", src.stats().reads, src.stats().bytes, src.stats().maxChunk)
	})
}

// TestConformanceOverlongHeader covers headers far longer than any
// reasonable line limit: the parser must surface the full header value
// through the public Header API instead of truncating or corrupting it.
func TestConformanceOverlongHeader(t *testing.T) {
	longValue := strings.Repeat("a", 128*1024)

	forEachAdapter(t, func(t *testing.T, ad adapter) {
		guard := newLeakGuard(t)
		defer guard.settle()

		body := buildForm(t, "boundary",
			partSpec{name: "field", headers: map[string]string{"X-Long-Header": longValue}, content: []byte("value")},
		)
		src := newStatReader(strings.NewReader(string(body)))

		h, err := ad.newHandle(src, "boundary")
		if err != nil {
			t.Fatalf("failed to create parser: %s", err)
		}

		if err := runParse(t, h, context.Background()); err != nil {
			t.Fatalf("parse failed: %s", err)
		}

		v, header, ok := h.value("field")
		if !ok {
			t.Fatal("field must be present")
		}
		if v != "value" {
			t.Fatalf("field content: expected %q, got %q", "value", v)
		}
		if got := header.Get("X-Long-Header"); got != longValue {
			t.Fatalf("overlong header: expected %d bytes, got %d bytes", len(longValue), len(got))
		}
	})
}

// TestConformanceUnicodeFilename covers non-ASCII file names in both the
// raw UTF-8 and the RFC 2231 filename*= forms.
func TestConformanceUnicodeFilename(t *testing.T) {
	const (
		rawName = "レポート-🚀-final.txt"
		// RFC 2231 encoding of "日本語-添付.txt"
		starName  = "日本語-添付.txt"
		starParam = "utf-8''%E6%97%A5%E6%9C%AC%E8%AA%9E-%E6%B7%BB%E4%BB%98.txt"
	)

	forEachAdapter(t, func(t *testing.T, ad adapter) {
		guard := newLeakGuard(t)
		defer guard.settle()

		body := buildForm(t, "boundary",
			partSpec{name: "raw", filename: rawName, contentType: "text/plain", content: []byte("raw")},
			partSpec{name: "star", filenameStar: starParam, contentType: "text/plain", content: []byte("star")},
		)
		src := newStatReader(strings.NewReader(string(body)))

		h, err := ad.newHandle(src, "boundary")
		if err != nil {
			t.Fatalf("failed to create parser: %s", err)
		}
		rawRec, starRec := &hookRecord{}, &hookRecord{}
		if err := h.register("raw", rawRec.hook()); err != nil {
			t.Fatalf("failed to register hook: %s", err)
		}
		if err := h.register("star", starRec.hook()); err != nil {
			t.Fatalf("failed to register hook: %s", err)
		}

		if err := runParse(t, h, context.Background()); err != nil {
			t.Fatalf("parse failed: %s", err)
		}

		if filenames, _ := rawRec.snapshot(); len(filenames) != 1 || filenames[0] != rawName {
			t.Fatalf("raw unicode filename: expected %q, got %v", rawName, filenames)
		}
		if filenames, _ := starRec.snapshot(); len(filenames) != 1 || filenames[0] != starName {
			t.Fatalf("rfc2231 filename: expected %q, got %v", starName, filenames)
		}
	})
}

// TestConformanceMultiFileSameField covers repeated part names: every file
// must reach the hook exactly once, in wire order, and repeated values must
// accumulate in order.
func TestConformanceMultiFileSameField(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, ad adapter) {
		guard := newLeakGuard(t)
		defer guard.settle()

		body := buildForm(t, "boundary",
			partSpec{name: "file", filename: "a.txt", contentType: "text/plain", content: []byte("content-a")},
			partSpec{name: "tag", content: []byte("first")},
			partSpec{name: "file", filename: "b.txt", contentType: "text/plain", content: []byte("content-b")},
			partSpec{name: "tag", content: []byte("second")},
		)
		src := newStatReader(strings.NewReader(string(body)))

		h, err := ad.newHandle(src, "boundary")
		if err != nil {
			t.Fatalf("failed to create parser: %s", err)
		}
		rec := &hookRecord{}
		if err := h.register("file", rec.hook()); err != nil {
			t.Fatalf("failed to register hook: %s", err)
		}

		if err := runParse(t, h, context.Background()); err != nil {
			t.Fatalf("parse failed: %s", err)
		}

		filenames, contents := rec.snapshot()
		if len(filenames) != 2 || filenames[0] != "a.txt" || filenames[1] != "b.txt" {
			t.Fatalf("hook filenames: expected [a.txt b.txt], got %v", filenames)
		}
		if len(contents) != 2 || string(contents[0]) != "content-a" || string(contents[1]) != "content-b" {
			t.Fatalf("hook contents out of order: %q", contents)
		}

		tags, ok := h.values("tag")
		if !ok || len(tags) != 2 || tags[0] != "first" || tags[1] != "second" {
			t.Fatalf("repeated values: expected [first second], got %v (ok=%v)", tags, ok)
		}
	})
}

// TestConformanceSlowConsumer covers a hook that consumes far slower than
// the producer could deliver: the parse must still complete with the exact
// content and the producer goroutine must exit.
func TestConformanceSlowConsumer(t *testing.T) {
	fileContent := strings.Repeat("slow-", 64*1024) // 320KB

	forEachAdapter(t, func(t *testing.T, ad adapter) {
		guard := newLeakGuard(t)
		defer guard.settle()

		body := buildForm(t, "boundary",
			partSpec{name: "file", filename: "slow.bin", contentType: "application/octet-stream", content: []byte(fileContent)},
		)
		src := newStatReader(strings.NewReader(string(body)))

		h, err := ad.newHandle(src, "boundary")
		if err != nil {
			t.Fatalf("failed to create parser: %s", err)
		}

		var consumed int64
		var mu sync.Mutex
		err = h.register("file", func(pr io.Reader, _ formstream.Header) error {
			n, err := io.Copy(myio.SlowWriter(), pr)
			mu.Lock()
			consumed = n
			mu.Unlock()
			return err
		})
		if err != nil {
			t.Fatalf("failed to register hook: %s", err)
		}

		if err := runParse(t, h, context.Background()); err != nil {
			t.Fatalf("parse failed: %s", err)
		}

		mu.Lock()
		got := consumed
		mu.Unlock()
		if got != int64(len(fileContent)) {
			t.Fatalf("slow consumer received %d bytes, expected %d", got, len(fileContent))
		}
		t.Logf("reads=%d bytes=%d peakChunk=%d", src.stats().reads, src.stats().bytes, src.stats().maxChunk)
	})
}

// errPrefixStop is returned by a consumer that only reads a prefix of the
// part it was handed.
var errPrefixStop = errors.New("consumer stopped after prefix")

// TestConformanceConsumerReadsPrefixOnly covers a consumer that stops
// after a prefix: the causal error must surface unchanged and the parser
// must stop pulling from the source instead of draining the whole file.
func TestConformanceConsumerReadsPrefixOnly(t *testing.T) {
	fileContent := strings.Repeat("x", 1<<20) // 1MB

	forEachAdapter(t, func(t *testing.T, ad adapter) {
		guard := newLeakGuard(t)
		defer guard.settle()

		body := buildForm(t, "boundary",
			partSpec{name: "file", filename: "big.bin", contentType: "application/octet-stream", content: []byte(fileContent)},
		)
		src := newStatReader(strings.NewReader(string(body)))

		h, err := ad.newHandle(src, "boundary")
		if err != nil {
			t.Fatalf("failed to create parser: %s", err)
		}
		err = h.register("file", func(pr io.Reader, _ formstream.Header) error {
			prefix := make([]byte, 512)
			if _, err := io.ReadFull(pr, prefix); err != nil {
				return err
			}
			return errPrefixStop
		})
		if err != nil {
			t.Fatalf("failed to register hook: %s", err)
		}

		err = runParse(t, h, context.Background())
		if !errors.Is(err, errPrefixStop) {
			t.Fatalf("expected the consumer's causal error, got: %v", err)
		}

		stats := src.stats()
		if stats.bytes >= int64(len(fileContent)) {
			t.Fatalf("parser drained the whole source (%d bytes) although the consumer stopped after a prefix", stats.bytes)
		}
		t.Logf("stopped early: reads=%d bytes=%d/%d peakChunk=%d", stats.reads, stats.bytes, len(body), stats.maxChunk)
	})
}
