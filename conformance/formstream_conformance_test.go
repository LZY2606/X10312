// Regression material for
// "formstream: cross-adapter streaming consistency and leak gating".
//
// Every case enters formstream through a public entry point (core package
// and the http/echo/gin adapters) and asserts both observable semantics
// (values, filenames, hook order, error class) and internal invariants
// (read/close counters, peak in-flight buffering, producer-goroutine
// exit, temp-file cleanup, no state carried over from a failed call).
package conformance_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mazrean/formstream/internal/conformance"
)

const (
	gateWait      = 2 * time.Second
	peakFastLimit = int64(1) << 20 // 1MiB streaming in-flight bound
	peakSlowLimit = int64(64) << 10
)

func adapters() []conformance.Adapter {
	return []conformance.Adapter{
		conformance.CoreAdapter{ValueKeys: valueKeys},
		conformance.HTTPAdapter{ValueKeys: valueKeys},
		conformance.EchoAdapter{ValueKeys: valueKeys},
		conformance.GinAdapter{ValueKeys: valueKeys},
	}
}

var valueKeys = []string{"field", "meta", "emptyfield"}

func runAllAdapters(t *testing.T, name string, attempts []conformance.AttemptConfig, want []conformance.ErrorClass, extra func(*conformance.Checker, conformance.AttemptResult)) {
	t.Helper()
	for _, a := range adapters() {
		a := a
		t.Run(a.Name()+"/"+name, func(t *testing.T) {
			for i, cfg := range attempts {
				tempBefore := countParserTempFiles()
				gate := conformance.GoroutineGate(t, gateWait)
				res := conformance.RunCase(t, a, i+1, cfg)
				gate()
				tempAfter := countParserTempFiles()

				c := conformance.NewChecker(t, a.Name(), name, i+1)
				c.CheckErrClass(res, want[i])
				c.Checkf(tempAfter <= tempBefore,
					"parser temp file leak: formstream-* count before=%d after=%d",
					tempBefore, tempAfter)
				if extra != nil {
					extra(c, res)
				}
				c.Report()
			}
		})
	}
}

// countParserTempFiles counts files left by the parser's preProcessor
// (prefix "formstream-"). The harness form files use a distinct
// "formstream-conformance-form-" prefix and are excluded.
func countParserTempFiles() int {
	matches, _ := filepath.Glob(filepath.Join(os.TempDir(), "formstream-*"))
	n := 0
	for _, m := range matches {
		if !strings.Contains(filepath.Base(m), "conformance-form") {
			n++
		}
	}
	return n
}
