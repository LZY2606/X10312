// Package conformance defines a single harness for exercising formstream
// through every public adapter.
//
// The same Scenario is executed against the root package, net/http, Echo and
// Gin entry points. Each run goes through the official public NewParser /
// Register / Parse surface only: the harness never constructs internal parser
// or pump objects directly. It additionally asserts the cross-cutting
// multipart/adapter/conformance invariants: bounded streaming memory,
// read/close accounting, producer goroutine shutdown and first-cause error
// preservation.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/mazrean/formstream"
)

// PartSpec describes one multipart part in a scenario.
type PartSpec struct {
	// Name is the form field name.
	Name string
	// Filename, when non-empty, makes the part a file part.
	Filename string
	// ContentType sets the Content-Type of file parts.
	ContentType string
	// Header carries extra raw headers.
	Header map[string]string
	// Data is the part body.
	Data []byte
	// DataReader builds the part body lazily when non-nil (takes precedence).
	DataReader func() (io.ReadCloser, int64)
}

// HookSpec declares how the consumer for a field behaves.
type HookSpec struct {
	// RequiredParts are the fields the hook depends on (abnormal path).
	RequiredParts []string
	// Limit, when >= 0, caps how many bytes the consumer reads.
	Limit int64
	// Delay pauses inside the consumer for slow-consumer coverage.
	Delay chan struct{}
	// ErrHook, when non-nil, is returned by the consumer.
	ErrHook error
}

// BodyMutation wraps the multipart body source.
type BodyMutation int

const (
	// BodyPlain delivers the body verbatim.
	BodyPlain BodyMutation = iota
	// BodyFailingReader injects ErrReaderFailure from the middle of the body.
	BodyFailingReader
	// BodySlow makes the body trickle out in small chunks.
	BodySlow
)

// ExpectClass classifies the observable result.
type ExpectClass string

const (
	ExpectSuccess         ExpectClass = "success"
	ExpectMalformed       ExpectClass = "malformed"
	ExpectBudgetParts     ExpectClass = "too-many-parts"
	ExpectBudgetHeaders   ExpectClass = "too-many-headers"
	ExpectBudgetMemory    ExpectClass = "too-large-form"
	ExpectHookFailure     ExpectClass = "hook-failure"
	ExpectReaderFailure   ExpectClass = "reader-failure"
	ExpectContextCanceled ExpectClass = "context-canceled"
)

// Options constrains parser resources for a scenario.
type Options struct {
	MaxParts       uint
	MaxHeaders     uint
	MaxMemSize     formstream.DataSize
	MaxMemFileSize formstream.DataSize
}

// Scenario is one fixed input and its observable + internal expectations.
type Scenario struct {
	// Name identifies the contract (shown in failing test names).
	Name string
	// Parts is the canonical multipart input.
	Parts []PartSpec
	// Hooks declares consumer behavior per field.
	Hooks map[string]HookSpec
	// Options constrains the parser.
	Options Options
	// Mutation wraps the request body.
	Mutation BodyMutation
	// CancelWhen, when non-nil, receives a trigger that cancels the parse
	// context. Hooks release Delay to signal they entered the consumer.
	CancelWhen func(trigger func())
	// SlowDelay blocks the consumer until closed (used with CancelWhen).
	SlowDelay chan struct{}
	// Expect is the required observable class.
	Expect ExpectClass
	// ExpectErr must satisfy errors.Is for the parse error (failure cases).
	ExpectErr error
	// ExpectValues asserts non-file field values after successful runs.
	ExpectValues map[string]string
	// ExpectFiles asserts observed file deliveries: field -> (filename -> count).
	ExpectFiles map[string]map[string]int
	// ExpectFileBytes asserts the exact delivered bytes of the named file field.
	ExpectFileBytes map[string][]byte
	// MaxPeakInflight bounds bytes ever buffered between producer and consumer.
	MaxPeakInflight int64
	// LargeBodySize, when > 0, declares the total body size and triggers the
	// no-whole-body-in-memory assertion against runtime heap growth.
	LargeBodySize int64
	// Heavy marks resource-heavy inputs skipped in -short mode.
	Heavy bool
}

// Result records what one adapter run observed.
type Result struct {
	Err          error
	Values       map[string]string
	Files        map[string][]fileDelivery
	FileOrder    []string
	ReadCalls    int64
	CloseCalls   int64
	PeakInflight int64
	Goroutines   int
	HeapAllocB   uint64
}

type fileDelivery struct {
	Filename string
	Content  []byte
}

// Adapter runs a scenario through one public entry point.
type Adapter interface {
	Name() string
	Run(ctx context.Context, sc Scenario) Result
}

// Failuref reports the first contract violation with context.
type Failuref func(format string, args ...any)

// Classify maps an error to an ExpectClass following the existing error
// hierarchy. Adapters must not invent alternative layers.
func Classify(err error) ExpectClass {
	switch {
	case err == nil:
		return ExpectSuccess
	case errors.Is(err, formstream.ErrTooManyParts):
		return ExpectBudgetParts
	case errors.Is(err, formstream.ErrTooManyHeaders):
		return ExpectBudgetHeaders
	case errors.Is(err, formstream.ErrTooLargeForm):
		return ExpectBudgetMemory
	case errors.Is(err, context.Canceled):
		return ExpectContextCanceled
	case errors.Is(err, ErrReaderFailure):
		return ExpectReaderFailure
	case errors.Is(err, ErrHookFailure):
		return ExpectHookFailure
	case isMalformed(err):
		return ExpectMalformed
	default:
		return ExpectClass(fmt.Sprintf("unknown(%v)", err))
	}
}

func isMalformed(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "multipart:")
}

// AssertResult checks observable semantics and key internal invariants.
func AssertResult(t testing.TB, adapterName string, sc Scenario, res Result) {
	t.Helper()

	class := Classify(res.Err)
	if class != sc.Expect {
		t.Errorf("[%s/%s] result class mismatch at error classification:\n"+
			"  scenario: %s\n  want: %s (%v)\n  got:  %s (%v)",
			adapterName, sc.Name, sc.Name, sc.Expect, sc.ExpectErr, class, res.Err)
		return
	}
	if sc.ExpectErr != nil && !errors.Is(res.Err, sc.ExpectErr) {
		t.Errorf("[%s/%s] causal error not preserved at errors.Is check:\n"+
			"  want errors.Is(err, %v)\n  got: %v",
			adapterName, sc.Name, sc.ExpectErr, res.Err)
	}

	if sc.Expect == ExpectSuccess {
		for key, want := range sc.ExpectValues {
			if got, ok := res.Values[key]; !ok || got != want {
				t.Errorf("[%s/%s] first value mismatch for field %q:\n  want: %q\n  got: %q (present=%v)",
					adapterName, sc.Name, key, want, got, ok)
			}
		}

		for field, wantFiles := range sc.ExpectFiles {
			got := res.Files[field]
			if len(got) == 0 {
				t.Errorf("[%s/%s] expected deliveries for file field %q, got none",
					adapterName, sc.Name, field)
				continue
			}
			counts := map[string]int{}
			for _, d := range got {
				counts[d.Filename]++
			}
			for name, wantCount := range wantFiles {
				if counts[name] != wantCount {
					t.Errorf("[%s/%s] file delivery count mismatch for %q/%q:\n  want: %d\n  got: %d",
						adapterName, sc.Name, field, name, wantCount, counts[name])
				}
			}
		}

		for field, wantBytes := range sc.ExpectFileBytes {
			got := res.Files[field]
			if len(got) != 1 {
				t.Errorf("[%s/%s] expected exactly 1 delivery for %q, got %d",
					adapterName, sc.Name, field, len(got))
				continue
			}
			if string(got[0].Content) != string(wantBytes) {
				t.Errorf("[%s/%s] file content mismatch for %q:\n  want %d bytes\n  got %d bytes",
					adapterName, sc.Name, field, len(wantBytes), len(got[0].Content))
			}
		}
	}

	if sc.MaxPeakInflight > 0 && res.PeakInflight > sc.MaxPeakInflight {
		t.Errorf("[%s/%s] producer buffered too much:\n  peak inflight: %d bytes\n  budget: %d bytes (whole body is %d bytes)",
			adapterName, sc.Name, res.PeakInflight, sc.MaxPeakInflight, sc.LargeBodySize)
	}

	if sc.LargeBodySize > 0 {
		if uint64(sc.LargeBodySize/2) < res.HeapAllocB {
			t.Errorf("[%s/%s] harness appears to hold the large body in memory:\n  body: %d bytes\n  heap alloc delta: %d bytes",
				adapterName, sc.Name, sc.LargeBodySize, res.HeapAllocB)
		}
	}
}

// AssertGoroutineFallback is the goroutine fallout gate: after every run the
// producer goroutine started by Parse must be gone. It waits briefly for a
// still-draining goroutine before failing, and reports the leaked stacks.
func AssertGoroutineFallback(t testing.TB, adapterName, scenarioName string, baseline int) {
	t.Helper()

	deadline := 0
	for {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		if deadline >= 100 {
			buf := make([]byte, 1<<20)
			n2 := runtime.Stack(buf, true)
			t.Fatalf("[%s/%s] goroutine fallout gate tripped:\n  baseline goroutines: %d\n  current: %d\n  stacks:\n%s",
				adapterName, scenarioName, baseline, n, buf[:n2])
		}
		deadline++
		runtime.Gosched()
	}
}

// Shared sentinels used by fixtures and assertions.
var (
	ErrReaderFailure = errors.New("conformance: injected reader failure")
	ErrHookFailure   = errors.New("conformance: consumer rejected part")
	ErrReaderRecover = errors.New("conformance: recoverable reader hiccup")
)

// Drain copies r fully, used by hooks that consume everything.
func Drain(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, 1<<20))
}
