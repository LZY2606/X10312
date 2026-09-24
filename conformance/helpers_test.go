package conformance_test

import (
	"time"

	"github.com/mazrean/formstream/internal/conformance"
)

// Consumer builds a one-file consumer used by the fixed corpus.
func Consumer(field string, spec conformance.FileSpec) conformance.Consumer {
	return conformance.Consumer{
		Files:     map[string]conformance.FileSpec{field: spec},
		SlowDelay: 200 * time.Microsecond,
	}
}

func fullFile() conformance.FileSpec {
	return conformance.FileSpec{Mode: conformance.ModeFull}
}

func slowFile() conformance.FileSpec {
	return conformance.FileSpec{Mode: conformance.ModeSlow}
}

func prefixFile(n int64) conformance.FileSpec {
	return conformance.FileSpec{Mode: conformance.ModePrefix, Prefix: n}
}

func errorFile(n int64) conformance.FileSpec {
	return conformance.FileSpec{Mode: conformance.ModeError, Prefix: n}
}
