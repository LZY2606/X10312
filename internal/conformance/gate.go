package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// GoroutineGate snapshots live goroutines and proves no parser-induced
// goroutine survives a parse. Unlike a generic leak check, it anchors on
// repository stack frames so a failure names the leaked production call site.
type GoroutineGate struct {
	before map[string]bool // stack signature -> true
}

type goroutineEntry struct {
	id    int
	state string
	stack string
}

// NewGoroutineGate takes a baseline. Fixture producer goroutines started
// afterwards are expected to disappear too.
func NewGoroutineGate() *GoroutineGate {
	return &GoroutineGate{before: goroutineStacks()}
}

func parseGoroutines(all string) []goroutineEntry {
	var entries []goroutineEntry
	for _, block := range strings.Split(all, "\n\n") {
		lines := strings.Split(block, "\n")
		if len(lines) < 2 {
			continue
		}
		// "goroutine 18 [chan send]:"
		h := lines[0]
		h = strings.TrimPrefix(h, "goroutine ")
		h = strings.TrimSuffix(h, ":")
		sp := strings.IndexByte(h, ' ')
		if sp < 0 {
			continue
		}
		id, err := strconv.Atoi(h[:sp])
		if err != nil {
			continue
		}
		state := strings.TrimSuffix(strings.TrimPrefix(h[sp+1:], "["), "]")
		entries = append(entries, goroutineEntry{
			id:    id,
			state: state,
			stack: strings.Join(lines[1:], "\n"),
		})
	}
	return entries
}

func goroutineStacks() map[string]bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	out := map[string]bool{}
	for _, e := range parseGoroutines(string(buf[:n])) {
		out[e.stack] = true
	}
	return out
}

// WaitNone blocks until every non-baseline goroutine is gone (or deadlines).
// The returned error names the first violating goroutine with its stack:
// failures report the contract breach position, never just a boolean.
func (g *GoroutineGate) WaitNone() error {
	deadline := time.Now().Add(3 * time.Second)
	var leaked []goroutineEntry
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		leaked = leaked[:0]
		for _, e := range parseGoroutines(string(buf[:n])) {
			if !g.before[e.stack] {
				// Only production frames (or a blocked io.Pipe producer)
				// count: harness/test frames themselves always carry the
				// repository import path and would otherwise false-positive.
				if strings.Contains(e.stack, "mazrean/formstream/internal/conformance.(*GoroutineGate)") {
					continue
				}
				if strings.Contains(e.stack, "mazrean/formstream/internal/conformance.pumpChunks") {
					leaked = append(leaked, e)
					continue
				}
				if isProductionFrame(e.stack) || strings.Contains(e.stack, "io.(*pipe)") {
					leaked = append(leaked, e)
				}
			}
		}
		if len(leaked) == 0 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	sort.Slice(leaked, func(i, j int) bool { return leaked[i].id < leaked[j].id })
	first := leaked[0]
	return fmt.Errorf("first goroutine leak: goroutine %d [%s] still alive after parse:\n%s",
		first.id, first.state, indent(first.stack))
}

// isProductionFrame reports whether the stack contains a non-harness
// repository frame: root package or a shipping adapter.
func isProductionFrame(stack string) bool {
	for _, marker := range []string{
		"mazrean/formstream.parse",
		"mazrean/formstream.(*Parser)",
		"mazrean/formstream/http",
		"mazrean/formstream/echo",
		"mazrean/formstream/gin",
		"mazrean/formstream/internal/condition_judge",
	} {
		if strings.Contains(stack, marker) {
			return true
		}
	}
	return false
}

func indent(s string) string {
	return "\t" + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n\t")
}

// AssertGoroutineGate runs a goroutine gate after fn and fails t on breach.
func AssertGoroutineGate(t testing.TB, fn func()) {
	t.Helper()
	gate := NewGoroutineGate()
	fn()
	if err := gate.WaitNone(); err != nil {
		t.Error(err)
	}
}

// TempFileGate snapshots formstream temp files and asserts none remain.
type TempFileGate struct {
	before map[string]bool
	seen   map[string]bool
	mu     sync.Mutex
	dir    string
}

func NewTempFileGate() *TempFileGate {
	dir := os.TempDir()
	return &TempFileGate{before: tempFiles(dir), seen: map[string]bool{}, dir: dir}
}

func tempFiles(dir string) map[string]bool {
	matches, _ := filepath.Glob(filepath.Join(dir, "formstream-*"))
	out := make(map[string]bool, len(matches))
	for _, m := range matches {
		out[m] = true
	}
	return out
}

// AssertReleased waits briefly for async cleanup, then fails on leftovers.
func (g *TempFileGate) AssertReleased(t testing.TB) {
	t.Helper()
	var leftovers []string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		g.Observe()
		for name := range tempFiles(g.dir) {
			if !g.before[name] {
				g.seen[name] = true
			}
		}
		leftovers = leftovers[:0]
		for name := range tempFiles(g.dir) {
			if !g.before[name] {
				leftovers = append(leftovers, name)
			}
		}
		if len(leftovers) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	sort.Strings(leftovers)
	t.Errorf("first temp-file leak: formstream temp files left on disk: %s", strings.Join(leftovers, ", "))
}

// sawSpool reports whether a formstream temp file existed at any point.
func (g *TempFileGate) sawSpool() bool {
	g.Observe()
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.seen) > 0
}

// Observe records formstream temp files currently present, so a file which
// is created and removed mid-parse still counts as spool evidence.
func (g *TempFileGate) Observe() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for name := range tempFiles(g.dir) {
		if !g.before[name] {
			g.seen[name] = true
		}
	}
}

// AssertTempFileCountStable checks no new formstream temp file survives.
func assertTempFileCountStable(t testing.TB, before map[string]bool) {
	t.Helper()
	for name := range tempFiles(os.TempDir()) {
		if !before[name] {
			t.Errorf("temp state leak: unexpected leftover %s after retry", name)
			return
		}
	}
}
