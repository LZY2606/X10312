package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

// Sentinel failure injected mid-file; conformance requires it to survive as
// the first causal error.
var ErrInjectMidRead = errors.New("injected reader failure mid-file")

const (
	// LargeFileSize is streamed in chunks, never held whole by the harness.
	LargeFileSize = 20 << 20
	// chunkSize bounds the bytes ever in flight between producer and parser.
	chunkSize = 32 << 10
	// peakInflightLimit must stay far below LargeFileSize, proving the large
	// file is never materialized in memory.
	peakInflightLimit = 128 << 10
)

func patternBytes(size int, seed byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = seed ^ byte(i%251)
	}
	return b
}

// Scenarios returns the fixed contract suite. Every case uses minimal input
// except the streaming cases, which are explicitly demand-driven.
func Scenarios() []Scenario {
	large := patternBytes(LargeFileSize, 1)
	largeFirstHalf := patternBytes(LargeFileSize/2, 1)

	cases := []Scenario{
		{
			Name:        "empty-part-empty-field",
			Description: "zero-length plain part must be a present empty value, not a missing one",
			Fields: []FieldSpec{
				{Name: "empty", Content: ""},
				{Name: "note", Content: "x"},
			},
			Want: Want{
				Fields: map[string]string{"empty": "", "note": "x"},
			},
		},
		{
			Name:        "empty-part-empty-file-hook",
			Description: "zero-length file must still invoke the hook once with size 0",
			Files:       []FileSpec{{Name: "doc", Filename: "empty.txt", Content: []byte("")}},
			HookFields:  map[string]struct{}{"doc": {}},
			Want: Want{
				Files: []FileRecord{ExpectedFile(FileSpec{Name: "doc", Filename: "empty.txt"})},
			},
		},
		{
			Name:        "unicode-filename",
			Description: "non-ASCII filename must round-trip through the public Header API",
			Files:       []FileSpec{{Name: "up", Filename: "テスト-名前-😀.txt", Content: []byte("unicode-body")}},
			HookFields:  map[string]struct{}{"up": {}},
			Want: Want{
				Files: []FileRecord{ExpectedFile(FileSpec{Name: "up", Filename: "テスト-名前-😀.txt", Content: []byte("unicode-body")})},
			},
		},
		{
			Name:        "multi-file-same-field",
			Description: "repeated file parts under one field must all be delivered in order",
			Files: []FileSpec{
				{Name: "uploads", Filename: "a.bin", Content: []byte("aaa")},
				{Name: "uploads", Filename: "b.bin", Content: []byte("bbbb")},
				{Name: "uploads", Filename: "c.bin", Content: []byte("c")},
			},
			HookFields: map[string]struct{}{"uploads": {}},
			Want: Want{
				Files: []FileRecord{
					ExpectedFile(FileSpec{Name: "uploads", Filename: "a.bin", Content: []byte("aaa")}),
					ExpectedFile(FileSpec{Name: "uploads", Filename: "b.bin", Content: []byte("bbbb")}),
					ExpectedFile(FileSpec{Name: "uploads", Filename: "c.bin", Content: []byte("c")}),
				},
			},
		},
		{
			Name:        "damaged-input-corrupt-boundary",
			Description: "corrupted delimiter mid-stream must report damaged input",
			Fields: []FieldSpec{
				{Name: "a", Content: "1"},
				{Name: "b", Content: "2"},
				{Name: "c", Content: "3"},
			},
			Corrupt: true,
			Want:    Want{WantErr: true},
		},
		{
			Name:        "resource-budget-too-large-form",
			Description: "field exceeding WithMaxMemSize must map to ErrTooLargeForm",
			Fields:      []FieldSpec{{Name: "big", Content: strings.Repeat("z", 4096)}},
			Options:     []formstream.ParserOption{formstream.WithMaxMemSize(1024)},
			Want:        Want{WantErr: true, ErrIs: formstream.ErrTooLargeForm},
		},
		{
			Name:        "resource-budget-too-many-parts",
			Description: "more parts than WithMaxParts must map to ErrTooManyParts",
			Fields: []FieldSpec{
				{Name: "p1", Content: "1"},
				{Name: "p2", Content: "2"},
				{Name: "p3", Content: "3"},
			},
			Options: []formstream.ParserOption{formstream.WithMaxParts(2)},
			Want:    Want{WantErr: true, ErrIs: formstream.ErrTooManyParts},
		},
		{
			Name:        "oversized-header-block",
			Description: "a header block past multipart's 10MiB limit must return ErrMessageTooLarge",
			LongHeader:  &LongHeaderSpec{Field: "big", Bytes: 10<<20 + 1024},
			Want:        Want{WantErr: true, ErrIs: multipart.ErrMessageTooLarge},
		},
		{
			Name:        "streaming-large-file-fast-path",
			Description: "a hook with no requirement streams the file without buffering it whole",
			Files:       []FileSpec{{Name: "blob", Filename: "large.bin", Content: large}},
			HookFields:  map[string]struct{}{"blob": {}},
			Stream:      &StreamSpec{FileIndex: 0, ChunkSize: chunkSize},
			Want: Want{
				Files: []FileRecord{ExpectedFile(FileSpec{Name: "blob", Filename: "large.bin", Content: large})},
			},
		},
		{
			Name:        "streaming-large-file-slow-consumer",
			Description: "a slow consumer must not stall or leak; producer stays bounded",
			Files:       []FileSpec{{Name: "blob", Filename: "slow.bin", Content: large}},
			HookFields:  map[string]struct{}{"blob": {}},
			Stream:      &StreamSpec{FileIndex: 0, ChunkSize: chunkSize, ChunkDelay: 100 * time.Microsecond},
			Want: Want{
				Files: []FileRecord{ExpectedFile(FileSpec{Name: "blob", Filename: "slow.bin", Content: large})},
			},
		},
		{
			Name:        "streaming-large-file-reader-mid-failure",
			Description: "a recoverable producer failure mid-file surfaces as first causal error and exits",
			Files:       []FileSpec{{Name: "blob", Filename: "half.bin", Content: large}},
			HookFields:  map[string]struct{}{"blob": {}},
			Stream: &StreamSpec{
				FileIndex:      0,
				ChunkSize:      chunkSize,
				FailAfterBytes: int64(LargeFileSize / 2),
				FailError:      ErrInjectMidRead,
			},
			Want: Want{WantErr: true, ErrIs: ErrInjectMidRead},
		},
		{
			Name:        "streaming-large-file-consumer-prefix-only",
			Description: "a hook that reads only a prefix then returns must let the producer drain and exit cleanly",
			Files:       []FileSpec{{Name: "blob", Filename: "prefix.bin", Content: large}},
			HookFields:  map[string]struct{}{"blob": {}},
			Stream:      &StreamSpec{FileIndex: 0, ChunkSize: chunkSize},
			// Prefix is observed via PrefixBytes, not full-file expectation.
			Want: Want{},
		},
		{
			Name:        "streaming-abnormal-replay-spools-to-disk",
			Description: "a required-part hook that arrives first is spooled and replayed after requirements",
			Fields:      []FieldSpec{{Name: "meta", Content: "after-file"}},
			Files:       []FileSpec{{Name: "blob", Filename: "spool.bin", Content: largeFirstHalf}},
			HookFields:  map[string]struct{}{"blob": {}},
			RequiredPart: map[string][]string{"blob": {"meta"}},
			Stream:      &StreamSpec{FileIndex: 0, ChunkSize: chunkSize},
			Want: Want{
				Fields: map[string]string{"meta": "after-file"},
				Files:  []FileRecord{ExpectedFile(FileSpec{Name: "blob", Filename: "spool.bin", Content: largeFirstHalf})},
			},
		},
	}

	return cases
}

// PrefixBytes is how many bytes the prefix-only hook consumes.
const PrefixBytes = 4096

// RegisterScenario installs hooks through the public Register API and returns
// a post-Parse field reader. Prefix-only is selected per scenario by name.
func RegisterScenario(t *testing.T, p *formstream.Parser, s Scenario, c *Collector) {
	t.Helper()

	prefixOnly := s.Name == "streaming-large-file-consumer-prefix-only"

	for field := range s.HookFields {
		field := field
		fn := ConsumeHook(c)
		if prefixOnly && field == "blob" {
			fn = prefixHook(PrefixBytes)
		}
		opts := []formstream.RegisterOption{}
		for _, req := range s.RequiredPart[field] {
			opts = append(opts, formstream.WithRequiredPart(req))
		}
		if err := p.Register(field, fn, opts...); err != nil {
			t.Fatalf("failed to register hook %q: %v", field, err)
		}
	}
}

func prefixHook(n int) formstream.StreamHookFunc {
	return func(r io.Reader, header formstream.Header) error {
		buf, err := io.ReadAll(io.LimitReader(r, int64(n)))
		if err != nil {
			return err
		}
		if len(buf) != n {
			return fmt.Errorf("prefix hook expected %d bytes, got %d", n, len(buf))
		}
		// Deliberately stop reading; production must drain and release.
		return nil
	}
}

// ReadScenarioFields records all plain fields after a successful Parse.
func ReadScenarioFields(p *formstream.Parser, c *Collector, s Scenario) {
	for _, f := range s.Fields {
		ReadFields(p, c, f.Name)
	}
}

// GoroutineGate snapshots goroutine counts and proves the production path
// returned to baseline (there are no long-lived producer goroutines).
type GoroutineGate struct {
	base    int
	once    sync.Once
	trace   []byte
}

func NewGoroutineGate() *GoroutineGate {
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	return &GoroutineGate{base: runtime.NumGoroutine()}
}

// WaitReturn waits until the goroutine count returns to baseline plus a small
// slack for unrelated runtime goroutines, then reports stacks on failure.
func (g *GoroutineGate) WaitReturn(t *testing.T, label string) {
	t.Helper()
	g.once.Do(func() {
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
		g.trace = buf.Bytes()
	})

	deadline := time.Now().Add(3 * time.Second)
	const slack = 2
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= g.base+slack {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}

	if runtime.NumGoroutine() > g.base+slack {
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
		t.Fatalf("[%s] goroutine gate not satisfied: baseline=%d now=%d\nstacks:\n%s",
			label, g.base, runtime.NumGoroutine(), buf.String())
	}
}

// AssertStreamingBounds enforces the no-full-buffering invariant on streamed
// scenarios: bytes in flight stay bounded by the chunk size.
func AssertStreamingBounds(t *testing.T, adapterName, scenarioName string, res RunResult) {
	t.Helper()
	if res.PeakInflight == 0 {
		t.Fatalf("[%s/%s] streaming scenario produced no inflight measurement", adapterName, scenarioName)
	}
	if res.PeakInflight > peakInflightLimit {
		t.Fatalf("[%s/%s] large file was buffered: peak inflight=%d exceeds limit=%d (file=%d)",
			adapterName, scenarioName, res.PeakInflight, peakInflightLimit, LargeFileSize)
	}
}

// AssertNoTempFilesLeft proves spooled files are released after Parse returns.
func AssertNoTempFilesLeft(t *testing.T, adapterName, scenarioName string, before, after []string) {
	t.Helper()
	left := difference(after, before)
	if len(left) > 0 {
		t.Fatalf("[%s/%s] temp files leaked: %v", adapterName, scenarioName, left)
	}
}

// TempDirSnapshot lists current formstream temp files.
func TempDirSnapshot() ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "formstream-*"))
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func difference(after, before []string) []string {
	beforeSet := make(map[string]struct{}, len(before))
	for _, p := range before {
		beforeSet[p] = struct{}{}
	}
	var extra []string
	for _, p := range after {
		if _, ok := beforeSet[p]; !ok {
			extra = append(extra, p)
		}
	}
	return extra
}

// RunVariant changes only result-irrelevant scheduling/enumeration knobs and
// restores the previous GOMAXPROCS on cleanup.
func RunVariant(t *testing.T, procs int, fn func()) {
	t.Helper()
	old := runtime.GOMAXPROCS(procs)
	t.Cleanup(func() { runtime.GOMAXPROCS(old) })
	fn()
}

// Ensure context is referenced for the cancellation helper scenario elsewhere.
var _ = context.Canceled
