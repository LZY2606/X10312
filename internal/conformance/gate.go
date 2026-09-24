package conformance

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Violation is a single contract violation with the first failing
// assertion location and the context needed to diagnose it.
type Violation struct {
	Where   string
	Message string
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s", v.Where, v.Message)
}

// Checker accumulates violations so a case reports the first violation
// location plus every other failing invariant instead of just a boolean.
type Checker struct {
	t          testing.TB
	adapter    string
	caseName   string
	attempt    int
	violations []Violation
}

// NewChecker starts a violation collector for one attempt.
func NewChecker(t testing.TB, adapter, caseName string, attempt int) *Checker {
	t.Helper()
	return &Checker{t: t, adapter: adapter, caseName: caseName, attempt: attempt}
}

func (c *Checker) mark() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", filepathBase(file), line)
}

// Checkf records a violation when ok is false.
func (c *Checker) Checkf(ok bool, format string, args ...any) {
	if ok {
		return
	}
	c.violations = append(c.violations, Violation{
		Where:   c.mark(),
		Message: fmt.Sprintf(format, args...),
	})
}

// CheckErrClass records a violation when the error class differs.
func (c *Checker) CheckErrClass(got AttemptResult, want ErrorClass) {
	if got.Class != want {
		c.Checkf(false, "error class: want %s, got %s (err=%v)", want, got.Class, got.Err)
	}
}

// Report fails the test with the first violation location and full
// context when any violation was recorded.
func (c *Checker) Report() {
	if len(c.violations) == 0 {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "conformance violation adapter=%s case=%s attempt=%d (%d violation(s)):\n",
		c.adapter, c.caseName, c.attempt, len(c.violations))
	for i, v := range c.violations {
		fmt.Fprintf(&b, "  %d. %s\n", i+1, v.String())
	}
	c.t.Errorf("%s", b.String())
}

func filepathBase(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

// GoroutineGate snapshots formstream-related goroutines and verifies the
// count returns to baseline after the parse attempt. This is the
// goroutine fallback gate: a producer/watcher goroutine that stays blocked
// fails the case deterministically instead of leaking.
func GoroutineGate(t testing.TB, wait time.Duration) func() {
	t.Helper()
	runtime.GC()
	baseline := countFormstreamGoroutines()
	return func() {
		t.Helper()
		deadline := time.Now().Add(wait)
		var (
			leaked int
			stacks string
		)
		for time.Now().Before(deadline) {
			runtime.GC()
			leaked, stacks = formstreamGoroutines(baseline)
			if leaked == 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Errorf("goroutine fallback gate: %d formstream producer/watcher goroutine(s) still alive after %s:\n%s",
			leaked, wait, stacks)
	}
}

func countFormstreamGoroutines() map[string]int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	blocks := bytes.Split(buf[:n], []byte("\n\n"))
	counts := make(map[string]int)
	for _, block := range blocks {
		if isFormstreamStack(block) {
			counts[stackKey(block)]++
		}
	}
	return counts
}

// formstreamGoroutines counts goroutines whose stack references the
// formstream parse path and returns stacks for the delta vs baseline.
func formstreamGoroutines(baseline map[string]int) (int, string) {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	blocks := bytes.Split(buf[:n], []byte("\n\n"))

	counts := make(map[string]int)
	var stacksFor func(key string) string
	stacksFor = func(key string) string {
		for _, block := range blocks {
			if bytes.Contains(block, []byte(key)) && isFormstreamStack(block) {
				return string(block)
			}
		}
		return key
	}

	for _, block := range blocks {
		if isFormstreamStack(block) {
			key := stackKey(block)
			counts[key]++
		}
	}

	total := 0
	var sb strings.Builder
	for key, n := range counts {
		extra := n - baseline[key]
		if extra <= 0 {
			continue
		}
		total += extra
		fmt.Fprintf(&sb, "x%d\n%s\n\n", extra, stacksFor(key))
	}
	return total, sb.String()
}

func isFormstreamStack(stack []byte) bool {
	return bytes.Contains(stack, []byte("mazrean/formstream.pipeProducer")) ||
		bytes.Contains(stack, []byte("mazrean/formstream.cancelWatcher")) ||
		bytes.Contains(stack, []byte("formstream.pipeProducer")) ||
		bytes.Contains(stack, []byte("formstream.cancelWatcher")) ||
		bytes.Contains(stack, []byte("formstream.(*Parser).ParseContext")) ||
		bytes.Contains(stack, []byte("formstream.(*Parser).parse("))
}

func stackKey(stack []byte) string {
	lines := bytes.Split(stack, []byte("\n"))
	if len(lines) >= 2 {
		return string(bytes.TrimSpace(lines[1]))
	}
	return string(stack)
}
