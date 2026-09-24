package conformance

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mazrean/formstream"
)

// Boundary is the multipart boundary used by every harness body.
const Boundary = "conformance-boundary-2026"

// ContentType is the Content-Type header value matching Boundary.
const ContentType = "multipart/form-data; boundary=" + Boundary

// Parser is the public surface the harness needs from every adapter parser.
type Parser interface {
	Register(name string, fn formstream.StreamHookFunc, options ...formstream.RegisterOption) error
	ParseWithContext(ctx context.Context) error
	Value(key string) (string, formstream.Header, bool)
	ValueMap() map[string][]formstream.Value
}

// Adapter builds parsers from a request body and Content-Type through the
// adapter's real public constructor.
type Adapter struct {
	Name string
	// NewParser enters the adapter public entry point.
	NewParser func(t TestingT, body io.Reader, contentType string, options ...formstream.ParserOption) Parser
	// NewParserErr enters the adapter constructor and returns its error.
	// It is used for entry-point validation and may be nil (core parser).
	NewParserErr func(t TestingT, body io.Reader, contentType string, options ...formstream.ParserOption) (Parser, error)
	// InvalidContentTypeError is the sentinel returned for a non-multipart
	// Content-Type.
	InvalidContentTypeError error
	// MissingBoundaryError is the sentinel returned when boundary is absent.
	MissingBoundaryError error
}

// Delivery is how the body reaches the parser.
type Delivery string

const (
	DeliveryStream Delivery = "stream"
	DeliveryMemory Delivery = "memory"
)

// FileObservation records what one file hook observed.
type FileObservation struct {
	HookName    string
	FileName    string
	ContentType string
	Size        int64
	SHA256      string
	// ReadError is the first error observed while consuming the file (before
	// the hook returns its own control error).
	ReadError error
}

// Result is everything observable from one parse run.
type Result struct {
	Err    error
	Source *Source
	Files  []FileObservation
	// Fields is the post-parse value map.
	Fields map[string][]formstream.Value
}

// fileHook configures how a registered hook consumes its parts.
type fileHook struct {
	name         string
	requireParts []string
	slow         bool
	prefixOnly   bool
	cancelOnData bool
}

type runConfig struct {
	parts      []PartSpec
	options    []formstream.ParserOption
	hooks      []fileHook
	streamOpts StreamOptions
	gate       bool
	// cancelCause, when non-nil, is used as the cause of context cancellation
	// (via context.WithCancelCause).
	cancelCause error
	// deadline, when positive, cancels the context after the given delay.
	deadline time.Duration
}

type violations []string

func (v *violations) addf(format string, args ...any) {
	*v = append(*v, fmt.Sprintf(format, args...))
}

// runOnce performs one parse through the public API and validates shared
// invariants: bounded reads, producer goroutine return and temp cleanup.
// Case-specific expectations are checked by the caller against the Result.
func runOnce(t TestingT, a Adapter, delivery Delivery, cfg runConfig) Result {
	t.Helper()

	label := fmt.Sprintf("[%s/%s]", a.Name, delivery)
	before := listFormstreamTemp(t)

	var src *Source
	switch delivery {
	case DeliveryStream:
		src = NewStream(cfg.parts, cfg.streamOpts)
	case DeliveryMemory:
		src = NewMemory(cfg.parts)
	default:
		t.Fatalf("%s unknown delivery: %s", label, delivery)
	}

	// WithCancelCause is used unconditionally: nil cause cancels with
	// context.Canceled, and hooks supply their own cause when required.
	var ctx context.Context
	causeCtx, cancel := context.WithCancelCause(context.Background())
	ctx = causeCtx
	if cfg.deadline > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeoutCause(ctx, cfg.deadline, context.DeadlineExceeded)
		defer cancelTimeout()
	}
	defer cancel(nil)

	parser := a.NewParser(t, src, ContentType, cfg.options...)

	res := Result{Source: src, Fields: map[string][]formstream.Value{}}
	res.Fields = map[string][]formstream.Value{}

	for _, h := range cfg.hooks {
		h := h
		err := parser.Register(h.name, func(r io.Reader, header formstream.Header) error {
			return consumeHook(r, header, h, &res, cfg, src, cancel)
		}, registerOptions(h.requireParts)...)
		if err != nil {
			t.Fatalf("%s Register(%q): %v", label, h.name, err)
		}
	}

	res.Err = parser.ParseWithContext(ctx)
	res.Fields = parser.ValueMap()

	var fails violations
	if delivery == DeliveryStream {
		if err := WaitProducer(src, 5*time.Second); err != nil {
			fails.addf("%s %v", label, err)
		}
		NewGoroutineGate().WaitForReturn(t, label)
		// The parser must pull in bounded chunks: the whole body is never
		// delivered in one Read. (multipart's peek buffer is chunkSize.)
		if src.Stats.MaxRead > chunkSize {
			fails.addf("%s max read %d exceeds bound %d; %s", label, src.Stats.MaxRead, chunkSize, src.Stats.Summary())
		}
		if src.Stats.TotalRead == 0 {
			fails.addf("%s parser never read the body; %s", label, src.Stats.Summary())
		}
	}
	if src.Stats.CloseCalls != 0 {
		fails.addf("%s parser closed the caller-provided reader %d time(s); parsers must never close the request body", label, src.Stats.CloseCalls)
	}
	if leaked := diffTemp(before, listFormstreamTemp(t)); len(leaked) != 0 {
		fails.addf("%s temporary formstream files left behind after run: %v", label, leaked)
	}

	for _, fail := range fails {
		t.Errorf("%s", fail)
	}

	return res
}

func consumeHook(r io.Reader, header formstream.Header, h fileHook, res *Result, cfg runConfig, src *Source, cancel context.CancelCauseFunc) error {
	obs := FileObservation{
		HookName:    h.name,
		FileName:    header.FileName(),
		ContentType: header.ContentType(),
	}

	hasher := sha256.New()
	var dst io.Writer = hasher
	if h.slow {
		dst = io.MultiWriter(hasher, &slowWriter{delay: 50 * time.Microsecond})
	}

	var max int64
	if h.prefixOnly {
		max = 2 * chunkSize
	}

	canceled := false
	copied, readErr := copyControlled(r, dst, max, func() {
		if cfg.gate {
			// The producer is blocked holding back the rest of this file
			// while we are already consuming it. Releasing here proves the
			// hook ran before the whole file was delivered.
			src.Release()
		}
		if h.cancelOnData && !canceled {
			canceled = true
			cancel(cfg.cancelCause)
		}
	})
	obs.Size = copied
	obs.SHA256 = fmt.Sprintf("%x", hasher.Sum(nil))
	obs.ReadError = readErr
	res.Files = append(res.Files, obs)

	switch {
	case h.prefixOnly:
		return ErrConsumerPrefixOnly
	case h.cancelOnData:
		if readErr != nil {
			return readErr
		}
		if cfg.cancelCause != nil {
			return cfg.cancelCause
		}
		return context.Canceled
	}
	return readErr
}

// copyControlled copies from r in small chunks, invoking onData after the first
// data is observed. At most max bytes are copied when max > 0.
func copyControlled(r io.Reader, w io.Writer, max int64, onData func()) (int64, error) {
	buf := make([]byte, 512)
	var total int64
	dataSeen := false
	for {
		if max > 0 && total >= max {
			return total, nil
		}
		limit := int64(len(buf))
		if max > 0 && total+limit > max {
			limit = max - total
		}
		n, err := r.Read(buf[:limit])
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
			if !dataSeen {
				dataSeen = true
				if onData != nil {
					onData()
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

type slowWriter struct {
	delay time.Duration
}

func (s *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(s.delay)
	return len(p), nil
}

func registerOptions(required []string) []formstream.RegisterOption {
	opts := make([]formstream.RegisterOption, 0, len(required))
	for _, name := range required {
		opts = append(opts, formstream.WithRequiredPart(name))
	}
	return opts
}

// expectError reports a violation unless the error chain of err contains want.
func expectError(t TestingT, label string, res Result, want error) {
	t.Helper()
	if want == nil {
		if res.Err != nil {
			t.Errorf("%s expected success, got error: %v; %s", label, res.Err, res.Source.Stats.Summary())
		}
		return
	}
	if res.Err == nil {
		t.Errorf("%s expected error %v, got success; %s", label, want, res.Source.Stats.Summary())
		return
	}
	if !errors.Is(res.Err, want) {
		t.Errorf("%s expected error chain to contain %v, got: %v", label, want, res.Err)
	}
}

func expectFields(t TestingT, label string, res Result, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	for key, values := range res.Fields {
		if len(values) == 0 {
			continue
		}
		content, _ := values[0].Unwrap()
		got[key] = content
	}
	if len(got) != len(want) {
		t.Errorf("%s field keys mismatch: want %v, got %v", label, sortedKeys(want), sortedKeys(got))
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s field %q = %q, want %q", label, key, got[key], value)
		}
	}
}

// fileExpectation is the expected observation for one file part.
type fileExpectation struct {
	HookName    string
	FileName    string
	ContentType string
	Size        int
	SHA256      string
}

func expectFiles(t TestingT, label string, res Result, want []fileExpectation) {
	t.Helper()
	if len(res.Files) != len(want) {
		t.Errorf("%s file observations = %d, want %d; observed: %s", label, len(res.Files), len(want), describeObs(res.Files))
		return
	}
	for i, w := range want {
		got := res.Files[i]
		if got.HookName != w.HookName || got.FileName != w.FileName || got.ContentType != w.ContentType ||
			got.Size != int64(w.Size) || got.SHA256 != w.SHA256 {
			t.Errorf("%s file[%d] = %+v, want {hook:%q file:%q type:%q size:%d sha:%q}",
				label, i, got, w.HookName, w.FileName, w.ContentType, w.Size, w.SHA256)
		}
	}
}

func describeObs(obs []FileObservation) string {
	parts := make([]string, 0, len(obs))
	for _, o := range obs {
		parts = append(parts, fmt.Sprintf("{hook:%q file:%q size:%d readErr:%v}", o.HookName, o.FileName, o.Size, o.ReadError))
	}
	return strings.Join(parts, ", ")
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func listFormstreamTemp(t TestingT) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	files := map[string]bool{}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "formstream-") {
			files[entry.Name()] = true
		}
	}
	return files
}

func diffTemp(before, after map[string]bool) []string {
	var leaked []string
	for name := range after {
		if !before[name] {
			leaked = append(leaked, filepath.Clean(name))
		}
	}
	sort.Strings(leaked)
	return leaked
}

// NewAdapterParser enters the adapter public entry point.
func NewAdapterParser(a Adapter, t TestingT, body io.Reader, contentType string, options ...formstream.ParserOption) Parser {
	return a.NewParser(t, body, contentType, options...)
}

// newAdapterParserError returns the constructor result for entry-error cases.
func newAdapterParserError(a Adapter, contentType string) (Parser, error) {
	return a.NewParserErr(nil, NewMemory([]PartSpec{FieldPart("a", "1")}), contentType)
}
