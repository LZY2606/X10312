// Package conformance runs the "formstream: cross-adapter streaming
// consistency and leak gate" contract through public entry points only.
//
// Cases never construct parser-internal objects: they build multipart bodies,
// enter via formstream.NewParser or an adapter NewParser, register hooks via
// Register and invoke Parse/ParseContext. The assertions inspect both the
// caller-visible outcome (error hierarchy, values, headers) and internal
// invariants observable on the production path (read/close counts, bytes
// held ahead of the consumer, producer liveness, temp-file release).
package conformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

// ParseCall invokes an adapter Parse/ParseContext entry point.
type ParseCall func(ctx context.Context) error

// ParserValues reads a value through the public Value API.
type ParserValues func(key string) (string, HeaderView, bool)

// HeaderView is the public Header surface relevant to the contract.
type HeaderView struct {
	Name        string
	Filename    string
	ContentType string
}

// Adapter abstracts one shipping public entry point.
type Adapter interface {
	Name() string
	NewParser(source io.ReadCloser, hooks []*HookSpec, opts []formstream.ParserOption) (ParseCall, ParserValues)
}

// Expect describes caller-visible outcome plus internal invariants.
type Expect struct {
	ParseErr      error
	CausalErrs    []error
	NotCausalErrs []error

	HookMinCalls map[string]int
	Values       map[string]string
	ValuesAbsent []string

	// MaxPeakInFlight bounds bytes buffered ahead of the active consumer.
	MaxPeakInFlight int64
	// ProducerExited requires the scripted source producer to terminate.
	ProducerExited bool
	// SourceClosed requires the parser to close the source on abort.
	SourceClosed bool
	// DiskSpool requires the slow path to create (and later remove) a temp file.
	DiskSpool bool
}

// Case is a fixed conformance input.
type Case struct {
	Name   string
	Body   Body
	Hooks  []*HookSpec
	Opts   []formstream.ParserOption
	Expect Expect
	Ctx    func() (context.Context, context.CancelFunc)
	// CancelAfter cancels the parse context after d, entering through
	// ParseContext (models request cancellation).
	CancelAfter time.Duration
}

// Registrar is the public Register function surface.
type Registrar func(name string, fn formstream.StreamHookFunc, opts ...formstream.RegisterOption) error

// Run executes c against adapter and fails with the first breach plus context.
func Run(t testing.TB, adapter Adapter, c Case) {
	t.Helper()

	tempGate := NewTempFileGate()
	goroutineGate := NewGoroutineGate()
	stopObserve := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tempGate.Observe()
			case <-stopObserve:
				return
			}
		}
	}()

	source := c.Body.NewSource()

	// Bind each hook to the ordered files sharing its name (multi-file case),
	// then let the adapter perform the public NewParser/Register sequence.
	for _, h := range c.Hooks {
		h.bind(c.Body)
	}
	parse, values := adapter.NewParser(source.Open(), c.Hooks, c.Opts)

	ctx := context.Background()
	if c.Ctx != nil {
		var cancel context.CancelFunc
		ctx, cancel = c.Ctx()
		defer cancel()
	}
	if c.CancelAfter > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		go func() {
			time.Sleep(c.CancelAfter)
			cancel()
		}()
		defer cancel()
	}

	err := parse(ctx)
	close(stopObserve)
	st := source.Stats()

	var breaches []string
	add := func(format string, args ...any) {
		breaches = append(breaches, fmt.Sprintf(format, args...))
	}

	switch {
	case c.Expect.ParseErr == nil && err != nil:
		add("parse returned unexpected error: %v", err)
	case c.Expect.ParseErr != nil && !errors.Is(err, c.Expect.ParseErr):
		add("parse error mismatch: want errors.Is(err, %v), got %v", c.Expect.ParseErr, err)
	}
	for _, want := range c.Expect.CausalErrs {
		if !errors.Is(err, want) {
			add("first causal error lost: want errors.Is(err, %v), got %v", want, err)
		}
	}
	for _, notWant := range c.Expect.NotCausalErrs {
		if errors.Is(err, notWant) {
			add("error chain must not contain %v, got %v", notWant, err)
		}
	}

	for _, h := range c.Hooks {
		calls := h.obs.snapshot()
		if want := c.Expect.HookMinCalls[h.Name]; len(calls) < want {
			add("hook %q invocations: want >= %d, got %d", h.Name, want, len(calls))
		}
		if fn := h.ObservedFilename; fn != "" && h.obs.filename != fn {
			add("hook %q filename: want %q, got %q", h.Name, fn, h.obs.filename)
		}
		if ct := h.ObservedType; ct != "" && h.obs.contentType != ct {
			add("hook %q content-type: want %q, got %q", h.Name, ct, h.obs.contentType)
		}
		for i, call := range calls {
			if want := h.ExpectedBytes; want >= 0 && call.bytes != want {
				add("hook %q invocation #%d consumed bytes: want %d, got %d", h.Name, i+1, want, call.bytes)
			}
			if h.ExpectContent {
				var file File
				if i < len(h.files) {
					file = h.files[i]
				}
				length := call.bytes
				if length != expectedReadLength(h, file) {
					add("hook %q invocation #%d content length mismatch: want %d, got %d",
						h.Name, i+1, expectedReadLength(h, file), length)
					continue
				}
				if call.hash != expectedHash(file, length) {
					add("hook %q invocation #%d received corrupted/misaligned file bytes", h.Name, i+1)
				}
			}
		}
	}

	if limit := c.Expect.MaxPeakInFlight; limit > 0 && st.PeakInFlight.Load() > limit {
		add("streaming footprint breach: peak bytes held ahead of consumer = %d, limit = %d (the whole large file must not be buffered)",
			st.PeakInFlight.Load(), limit)
	}
	if c.Expect.ProducerExited && !waitProducer(source, 3*time.Second) {
		add("producer goroutine did not exit after parse returned: reads=%d closes=%d bytes=%d",
			st.ReadCalls.Load(), st.CloseCalls.Load(), st.BytesRead.Load())
	}
	if c.Expect.SourceClosed && st.CloseCalls.Load() == 0 {
		add("source was not released on abort (close calls=0): a pipe/HTTP producer would block forever")
	}

	if err == nil {
		for key, want := range c.Expect.Values {
			got, _, ok := values(key)
			if !ok {
				add("value %q not observable through public Value API", key)
			} else if got != want {
				add("value %q: want %q, got %q", key, want, got)
			}
		}
		for _, key := range c.Expect.ValuesAbsent {
			if got, _, ok := values(key); ok {
				add("value %q must not be present, got %q", key, got)
			}
		}
	}

	if leak := goroutineGate.WaitNone(); leak != nil {
		add("%s", leak)
	}
	tempGate.AssertReleased(t)
	if c.Expect.DiskSpool && !tempGate.sawSpool() {
		add("slow path was expected to spool a temp file, but none was created")
	}

	if len(breaches) > 0 {
		var sb strings.Builder
		sb.WriteString("first violation: " + breaches[0])
		if len(breaches) > 1 {
			sb.WriteString(fmt.Sprintf("\n(+%d more)", len(breaches)-1))
			for _, b := range breaches[1:] {
				sb.WriteString("\n  - " + b)
			}
		}
		fmt.Fprintf(&sb, "\ncontext: adapter=%s case=%s reads=%d bytes=%d closes=%d peakInFlight=%d err=%v",
			adapter.Name(), c.Name, st.ReadCalls.Load(), st.BytesRead.Load(),
			st.CloseCalls.Load(), st.PeakInFlight.Load(), err)
		t.Error(sb.String())
	}
	t.Logf("case=%s adapter=%s reads=%d bytes=%d closes=%d peakInFlight=%d parseErr=%v",
		c.Name, adapter.Name(), st.ReadCalls.Load(), st.BytesRead.Load(),
		st.CloseCalls.Load(), st.PeakInFlight.Load(), err)
}

func waitProducer(s *Source, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if s.PumpDone() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return s.PumpDone()
}
