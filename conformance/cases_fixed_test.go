package conformance_test

import (
	"context"
	"testing"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/conformance"
)

// TestFormStreamConformance_EmptyParts covers an empty field and an empty
// file part: a common broken implementation either drops the field or
// reports a spurious read error on zero-length files.
func TestFormStreamConformance_EmptyParts(t *testing.T) {
	runAllAdapters(t, "emptyparts",
		[]conformance.AttemptConfig{{
			Form:     emptyPartsForm(),
			Consumer: Consumer("emptyfile", fullFile()),
		}},
		[]conformance.ErrorClass{conformance.ClassNone},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			c.Checkf(r.Snapshot["emptyfield"] == "", "empty field: got %q want %q", r.Snapshot["emptyfield"], "")
			c.Checkf(len(r.Files) == 1, "empty file hook calls: got %d want 1", len(r.Files))
			if len(r.Files) == 1 {
				f := r.Files[0]
				c.Checkf(f.Bytes == 0, "empty file bytes: got %d want 0", f.Bytes)
				c.Checkf(f.SHA256 == conformance.PatternChecksum("emptyfile", 0), "empty file hash mismatch")
			}
			c.Checkf(r.Meter.Closes() == 1, "source close count: got %d want 1", r.Meter.Closes())
		})
}

// TestFormStreamConformance_HugeSingleHeader covers an oversized header
// line as malformed input.
func TestFormStreamConformance_HugeSingleHeader(t *testing.T) {
	runAllAdapters(t, "hugesingleheader",
		[]conformance.AttemptConfig{{
			Form:     hugeSingleHeaderForm(),
			Consumer: Consumer("dense", fullFile()),
		}},
		[]conformance.ErrorClass{conformance.ClassMalformed},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			c.Checkf(r.Meter.PeakInflight() < int64(2<<20),
				"oversized header buffered: peakInflight=%d should stay below the 2MiB header",
				r.Meter.PeakInflight())
		})
}

// TestFormStreamConformance_TooManyHeaders covers header-budget exhaustion
// and asserts the existing ErrTooManyHeaders layering is preserved.
func TestFormStreamConformance_TooManyHeaders(t *testing.T) {
	runAllAdapters(t, "toomanyheaders",
		[]conformance.AttemptConfig{{
			Form:     tooManyHeadersForm(10),
			Consumer: Consumer("dense", fullFile()),
			Options:  conformance.ParseOptions{MaxHeaders: 5},
		}},
		[]conformance.ErrorClass{conformance.ClassTooManyHeaders},
		nil)
}

// TestFormStreamConformance_UnicodeFilename covers non-ASCII filenames
// round-tripping identically through every adapter.
func TestFormStreamConformance_UnicodeFilename(t *testing.T) {
	runAllAdapters(t, "unicodefilename",
		[]conformance.AttemptConfig{{
			Form:     basicSuccessForm(),
			Consumer: Consumer("avatar", fullFile()),
		}},
		[]conformance.ErrorClass{conformance.ClassNone},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			c.Checkf(r.Snapshot["field"] == "mazrean", "field: got %q", r.Snapshot["field"])
			if len(r.Files) != 1 {
				c.Checkf(false, "file hook calls: got %d want 1", len(r.Files))
				return
			}
			f := r.Files[0]
			c.Checkf(f.FileName == "テスト ファイル.png", "unicode filename: got %q", f.FileName)
			c.Checkf(f.ContentType == "image/png", "content type: got %q", f.ContentType)
			c.Checkf(f.Bytes == smallFile, "bytes: got %d want %d", f.Bytes, smallFile)
			c.Checkf(f.SHA256 == conformance.PatternChecksum("avatar", smallFile), "file content hash mismatch")
		})
}

// TestFormStreamConformance_MultipleFilesSameField covers several files on
// one field: the hook must fire once per file in wire order with complete
// contents.
func TestFormStreamConformance_MultipleFilesSameField(t *testing.T) {
	runAllAdapters(t, "multiplefilessamefield",
		[]conformance.AttemptConfig{{
			Form:     multiFileForm(),
			Consumer: Consumer("docs", fullFile()),
		}},
		[]conformance.ErrorClass{conformance.ClassNone},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			c.Checkf(len(r.Files) == 2, "file hook calls: got %d want 2", len(r.Files))
			if len(r.Files) == 2 {
				want := []struct {
					name string
					size int64
				}{{"a.txt", 1024}, {"b.txt", 2048}}
				for i, w := range want {
					f := r.Files[i]
					c.Checkf(f.Order == i+1, "order: got %d want %d", f.Order, i+1)
					c.Checkf(f.FileName == w.name, "file[%d] name: got %q want %q", i, f.FileName, w.name)
					c.Checkf(f.Bytes == w.size, "file[%d] bytes: got %d want %d", i, f.Bytes, w.size)
					c.Checkf(f.SHA256 == conformance.PatternChecksum("docs", w.size),
						"file[%d] hash mismatch", i)
				}
			}
			c.Checkf(r.Snapshot["meta"] == "meta-value", "meta field: got %q", r.Snapshot["meta"])
		})
}

// TestFormStreamConformance_SlowConsumer streams a large file while the
// consumer drains slowly: in-flight buffering must stay bounded and the
// producer goroutine must exit at the end.
func TestFormStreamConformance_SlowConsumer(t *testing.T) {
	runAllAdapters(t, "slowconsumer",
		[]conformance.AttemptConfig{{
			Form:     reversedForm(),
			Consumer: Consumer("upload", slowFile()),
		}},
		[]conformance.ErrorClass{conformance.ClassNone},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			c.Checkf(r.Snapshot["field"] == "later", "required field after file: got %q", r.Snapshot["field"])
			if len(r.Files) == 1 {
				f := r.Files[0]
				c.Checkf(f.Bytes == largeFile, "slow-consumed bytes: got %d want %d", f.Bytes, largeFile)
				c.Checkf(f.SHA256 == conformance.PatternChecksum("upload", largeFile), "slow-consumed hash mismatch")
			}
			c.Checkf(r.Meter.PeakInflight() < peakFastLimit,
				"slow consumer buffered too much: peakInflight=%d limit=%d",
				r.Meter.PeakInflight(), peakFastLimit)
		})
}

// TestFormStreamConformance_ReaderFailsMidstream injects a recoverable
// source failure partway through a large file: the first causal error must
// be preserved, the producer must exit, and a retry with the identical form
// must succeed without any cached/counter residue.
func TestFormStreamConformance_ReaderFailsMidstream(t *testing.T) {
	cfg := func(fault bool) conformance.AttemptConfig {
		c := conformance.AttemptConfig{
			Form:     reversedForm(),
			Consumer: Consumer("upload", fullFile()),
		}
		if fault {
			c.ReaderFault = &conformance.ReaderFault{FailAfterBytes: largeFile / 2, Attempt: 1}
		}
		return c
	}
	runAllAdapters(t, "readerfailsmidstream",
		[]conformance.AttemptConfig{cfg(true), cfg(false)},
		[]conformance.ErrorClass{conformance.ClassReaderFailure, conformance.ClassNone},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			if r.Class == conformance.ClassReaderFailure {
				c.Checkf(r.Meter.Produced() < largeFile,
					"abort should stop source reads: produced=%d form file size=%d",
					r.Meter.Produced(), largeFile)
				return
			}
			c.Checkf(r.Snapshot["field"] == "later", "retry field: got %q", r.Snapshot["field"])
			if len(r.Files) == 1 {
				f := r.Files[0]
				c.Checkf(f.Bytes == largeFile, "retry bytes: got %d want %d", f.Bytes, largeFile)
				c.Checkf(f.SHA256 == conformance.PatternChecksum("upload", largeFile), "retry hash mismatch")
			}
			// The failed attempt must not have left the meter/counters
			// affecting this fresh parser result.
			c.Checkf(r.Meter.Closes() == 1, "retry source close count: got %d want 1", r.Meter.Closes())
		})
}

// TestFormStreamConformance_ConsumerReadsPrefix covers a hook that reads
// only a prefix and returns nil. The parser must drain the rest of the
// part and finish cleanly; the producer must exit.
func TestFormStreamConformance_ConsumerReadsPrefix(t *testing.T) {
	runAllAdapters(t, "consumerreadsprefix",
		[]conformance.AttemptConfig{{
			Form:     basicSuccessForm(),
			Consumer: Consumer("avatar", prefixFile(64)),
		}},
		[]conformance.ErrorClass{conformance.ClassNone},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			if len(r.Files) == 1 {
				f := r.Files[0]
				c.Checkf(f.Bytes == 64, "prefix bytes: got %d want 64", f.Bytes)
				c.Checkf(f.SHA256 == conformance.PatternChecksum("avatar", 64), "prefix hash mismatch")
			}
			c.Checkf(r.Meter.Closes() == 1, "source close count: got %d want 1", r.Meter.Closes())
		})
}

// TestFormStreamConformance_ConsumerFails covers a hook that reads a prefix
// then returns an error. This is the "common wrong implementation" trap:
// swallowing the hook error or deadlocking the producer both fail here.
func TestFormStreamConformance_ConsumerFails(t *testing.T) {
	runAllAdapters(t, "consumerfails",
		[]conformance.AttemptConfig{{
			Form:     basicSuccessForm(),
			Consumer: Consumer("avatar", errorFile(64)),
		}},
		[]conformance.ErrorClass{conformance.ClassConsumerFailure},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			if len(r.Files) == 1 {
				c.Checkf(r.Files[0].Bytes == 64, "failed hook bytes: got %d want 64", r.Files[0].Bytes)
			}
		})
}

// TestFormStreamConformance_ContextCancel cancels while a large file is
// streaming: the context cause is preserved and the producer exits.
func TestFormStreamConformance_ContextCancel(t *testing.T) {
	runAllAdapters(t, "contextcancel",
		[]conformance.AttemptConfig{{
			Form:     reversedForm(),
			Consumer: Consumer("upload", slowFile()),
			Cancel: func(cancel context.CancelCauseFunc) {
				cancel(context.Canceled)
			},
		}},
		[]conformance.ErrorClass{conformance.ClassContextCancelled},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			c.Checkf(r.Meter.Produced() < largeFile,
				"cancelled parse should stop reads: produced=%d form=%d",
				r.Meter.Produced(), largeFile)
		})
}

// TestFormStreamConformance_MalformedInput keeps malformed bodies in the
// malformed-input class, distinct from budget errors.
func TestFormStreamConformance_MalformedInput(t *testing.T) {
	runAllAdapters(t, "malformedinput",
		[]conformance.AttemptConfig{{
			Form:     malformedForm(),
			Consumer: Consumer("field", fullFile()),
		}},
		[]conformance.ErrorClass{conformance.ClassMalformed},
		nil)
}

// TestFormStreamConformance_BudgetExhausted covers the memory and parts
// budgets, each keeping its own sentinel error class.
func TestFormStreamConformance_BudgetExhausted(t *testing.T) {
	runAllAdapters(t, "budget-memory",
		[]conformance.AttemptConfig{{
			Form:     hugeFieldForm(4096),
			Consumer: Consumer("unhooked", fullFile()),
			Options:  conformance.ParseOptions{MaxMemSize: formstream.KB},
		}},
		[]conformance.ErrorClass{conformance.ClassTooLargeForm},
		nil)

	runAllAdapters(t, "budget-parts",
		[]conformance.AttemptConfig{{
			Form:     manyPartsForm(4),
			Consumer: Consumer("none", fullFile()),
			Options:  conformance.ParseOptions{MaxParts: 2},
		}},
		[]conformance.ErrorClass{conformance.ClassTooManyParts},
		nil)
}

// TestFormStreamConformance_SuccessAfterFailure proves a successful result
// on a fresh parser carries no residue from a previous failed call: same
// input, stable, complete outcome.
func TestFormStreamConformance_SuccessAfterFailure(t *testing.T) {
	faultCfg := conformance.AttemptConfig{
		Form:        multiFileForm(),
		Consumer:    Consumer("docs", errorFile(32)),
		ReaderFault: nil,
	}
	okCfg := conformance.AttemptConfig{
		Form:     multiFileForm(),
		Consumer: Consumer("docs", fullFile()),
	}
	runAllAdapters(t, "successafterfailure",
		[]conformance.AttemptConfig{faultCfg, okCfg},
		[]conformance.ErrorClass{conformance.ClassConsumerFailure, conformance.ClassNone},
		func(c *conformance.Checker, r conformance.AttemptResult) {
			if r.Class != conformance.ClassNone {
				return
			}
			c.Checkf(len(r.Files) == 2, "post-failure file calls: got %d want 2", len(r.Files))
			if len(r.Files) == 2 {
				c.Checkf(r.Files[0].Bytes == 1024 && r.Files[1].Bytes == 2048,
					"post-failure sizes: %d,%d", r.Files[0].Bytes, r.Files[1].Bytes)
			}
			c.Checkf(r.Snapshot["meta"] == "meta-value", "post-failure meta: got %q", r.Snapshot["meta"])
		})
}
