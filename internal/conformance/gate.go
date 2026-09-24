package conformance

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// TestingT is the subset of *testing.T the harness needs.
type TestingT interface {
	Helper()
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Run(name string, f func(TestingT)) bool
	Cleanup(func())
}

// producerMarkers identify stacks owned by form production: the harness form
// producer and the library's ParseWithContext pipe producer.
var producerMarkers = []string{
	"conformanceFormProducer",
	"pipeReaderToWriter",
}

// GoroutineGate waits for producer goroutines to return after a run.
//
// It inspects goroutine stacks rather than a harness-local WaitGroup so a
// leaked library-side producer (which the harness cannot coordinate with) is
// detected too.
type GoroutineGate struct {
	allowed time.Duration
}

func NewGoroutineGate() *GoroutineGate {
	return &GoroutineGate{allowed: 3 * time.Second}
}

// WaitForReturn waits for producer goroutines to exit and reports the first
// violation together with the retained goroutine stacks as context.
func (g *GoroutineGate) WaitForReturn(t TestingT, scenario string) {
	t.Helper()

	deadline := time.Now().Add(g.allowed)
	for time.Now().Before(deadline) {
		if !producerGoroutinesAlive() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Errorf("%s: producer goroutine did not return within %s; retained stacks:\n%s",
		scenario, g.allowed, goroutineStacksContaining(producerMarkers))
}

func producerGoroutinesAlive() bool {
	return goroutineStacksContaining(producerMarkers) != ""
}

func goroutineStacksContaining(markers []string) string {
	raw := make([]byte, 1<<20)
	for {
		n := runtime.Stack(raw, true)
		if n < len(raw) {
			raw = raw[:n]
			break
		}
		raw = make([]byte, 2*len(raw))
	}

	var matched bytes.Buffer
	for _, stack := range bytes.Split(raw, []byte("\n\n")) {
		text := string(stack)
		for _, marker := range markers {
			if strings.Contains(text, marker) {
				fmt.Fprintf(&matched, "--- goroutine matches %q ---\n%s\n\n", marker, text)
				break
			}
		}
	}
	return matched.String()
}
