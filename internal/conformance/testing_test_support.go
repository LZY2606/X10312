package conformance

import "testing"

// testingTRunner adapts *testing.T subtests to the harness TestingT interface.
// This file is regular (non-test) code because every adapter package imports
// the harness from its own _test package; the harness itself contains no
// production behavior, only test infrastructure.
type testingTRunner struct {
	*testing.T
}

// RunTest executes fn with a TestingT backed by *testing.T subtests.
func RunTest(t *testing.T, fn func(TestingT)) {
	t.Helper()
	fn(testingTRunner{t})
}

func (t testingTRunner) Run(name string, f func(TestingT)) bool {
	return t.T.Run(name, func(st *testing.T) {
		f(testingTRunner{st})
	})
}
