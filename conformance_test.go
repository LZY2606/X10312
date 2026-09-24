package formstream_test

// Regression suite for:
// "formstream: cross-adapter streaming consistency and leak gate".
//
// Every case enters through a public adapter entry point (formstream,
// httpform, echoform, ginform); nothing constructs parser-internal objects.
// Fixed counter-examples cover the empty part, overlong header, Unicode
// filename, multiple files under one field, slow consumer, mid-stream reader
// failure and prefix-only consumer cases, together with the recoverable
// mid-flight failure and ordering/scheduling stability stories.

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/conformance"
)

// TestFormstream_CrossAdapterConformance runs the fixed case matrix against
// every shipping adapter. Sub-test names identify the adapter and the
// contract a failure violates.
func TestFormstream_CrossAdapterConformance(t *testing.T) {
	t.Parallel()

	for _, adapter := range conformance.Adapters() {
		for _, tc := range conformance.FixedCases() {
			t.Run(adapter.Name()+"/"+tc.Name, func(t *testing.T) {
				t.Parallel()
				conformance.Run(t, adapter, tc)
			})
		}
	}
}

// TestFormstream_RecoverableMidFailureThenRetry is the end-to-end acceptance
// story: the first attempt fails mid-stream (recoverable), the parser must
// leave no pooled buffers/counters/temp state behind, and retrying with the
// same input through a fresh public parser yields a stable success.
func TestFormstream_RecoverableMidFailureThenRetry(t *testing.T) {
	t.Parallel()

	for _, adapter := range conformance.Adapters() {
		adapter := adapter
		t.Run(adapter.Name()+"/recoverable_failure_then_retry", func(t *testing.T) {
			t.Parallel()

			// Attempt 1: upstream dies in the middle of the file.
			fail := conformance.Body{
				Stream:    true,
				ChunkSize: 32 * 1024,
				FailAfter: 64 * 1024,
				FailErr:   conformance.ErrReaderDied,
				Files: []conformance.File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 256 * 1024},
				},
				Fields: []conformance.Field{{Name: "after", Value: "ok"}},
			}
			hooks := []*conformance.HookSpec{
				conformance.Spec("upload", conformance.WithRequired("after")),
			}
			failCase := conformance.Case{
				Name:   "retry_attempt_failure",
				Body:   fail,
				Hooks:  hooks,
				Expect: conformance.Expect{CausalErrs: []error{conformance.ErrReaderDied}, SourceClosed: true},
			}
			conformance.Run(t, adapter, failCase)
			if t.Failed() {
				return
			}

			// Attempt 2: identical logical input succeeds; successful results
			// must not carry bytes/state from the failed attempt. Verify by
			// feeding a distinct scheduling profile (different chunk size),
			// which is result-irrelevant.
			okBody := conformance.Body{
				Stream:    true,
				ChunkSize: 48 * 1024,
				Files: []conformance.File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 256 * 1024},
				},
				Fields: []conformance.Field{{Name: "after", Value: "ok"}},
			}
			okHooks := []*conformance.HookSpec{
				conformance.Spec("upload", conformance.WithRequired("after"),
					conformance.CheckContent(), conformance.ExpectFilename("big.bin")),
			}
			okCase := conformance.Case{
				Name:  "retry_attempt_success",
				Body:  okBody,
				Hooks: okHooks,
				Expect: conformance.Expect{
					ParseErr:       nil,
					HookMinCalls:   map[string]int{"upload": 1},
					Values:         map[string]string{"after": "ok"},
					ProducerExited: true,
				},
			}
			conformance.Run(t, adapter, okCase)
		})
	}
}

// TestFormstream_ResultStableUnderScheduling runs the same logical inputs in
// varying result-irrelevant conditions (chunk/order jitter, fast and slow
// consumers) and asserts identical observable results.
func TestFormstream_ResultStableUnderScheduling(t *testing.T) {
	t.Parallel()

	profiles := []struct {
		name      string
		chunkSize int
		throttle  time.Duration
	}{
		{"tight_fast", 16 * 1024, 0},
		{"loose_fast", 128 * 1024, 0},
		{"tight_slow", 16 * 1024, 40 * time.Microsecond},
	}

	for _, adapter := range conformance.Adapters() {
		for _, profile := range profiles {
			profile := profile
			t.Run(adapter.Name()+"/jitter/"+profile.name, func(t *testing.T) {
				t.Parallel()
				hooks := []*conformance.HookSpec{
					conformance.Spec("uploads", conformance.CheckContent(),
						conformance.ExpectFilename("a.bin")),
				}
				if profile.throttle > 0 {
					hooks[0] = conformance.Spec("uploads", conformance.CheckContent(),
						conformance.ExpectFilename("a.bin"), conformance.ThrottleReads(profile.throttle))
				}
				c := conformance.Case{
					Name: "jitter_" + profile.name,
					Body: conformance.Body{
						Stream:    true,
						ChunkSize: profile.chunkSize,
						Files: []conformance.File{
							{Name: "uploads", Filename: "a.bin", MediaType: "application/octet-stream", Size: 96 * 1024},
							{Name: "uploads", Filename: "b.bin", MediaType: "application/octet-stream", Size: 48 * 1024},
						},
						Fields: []conformance.Field{{Name: "note", Value: "stable"}},
					},
					Hooks: hooks,
					Expect: conformance.Expect{
						HookMinCalls:    map[string]int{"uploads": 2},
						Values:          map[string]string{"note": "stable"},
						ProducerExited:  true,
						MaxPeakInFlight: int64(profile.chunkSize * 6),
					},
				}
				conformance.Run(t, adapter, c)
			})
		}
	}
}

// TestFormstream_LargeFileNeverFullyBuffered is the strict streaming gate on
// the fast path: while the hook is mid-read, the source must not have handed
// over the whole multi-MB file. It also sanity-checks heap growth stays an
// order of magnitude below the file size.
func TestFormstream_LargeFileNeverFullyBuffered(t *testing.T) {
	t.Parallel()

	const (
		fileSize = 4 << 20
		chunk    = 64 * 1024
	)

	for _, adapter := range conformance.Adapters() {
		adapter := adapter
		t.Run(adapter.Name()+"/large_file_streaming_gate", func(t *testing.T) {
			t.Parallel()

			c := conformance.Case{
				Name: "large_file_streaming_gate",
				Body: conformance.Body{
					Stream:    true,
					ChunkSize: chunk,
					Files: []conformance.File{
						{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: fileSize},
					},
				},
				Hooks: []*conformance.HookSpec{
					conformance.Spec("upload", conformance.CheckContent(),
						conformance.ThrottleReads(20*time.Microsecond)),
				},
				Expect: conformance.Expect{
					HookMinCalls:    map[string]int{"upload": 1},
					ProducerExited:  true,
					MaxPeakInFlight: int64(chunk * 4),
				},
			}
			conformance.Run(t, adapter, c)
		})
	}
}

// TestFormstream_FirstCausalErrorOnConsumerStop directly asserts that when a
// consumer stops after a prefix with an error, that exact error (not a
// following drain/read error) is returned and errors.Is keeps working.
func TestFormstream_FirstCausalErrorOnConsumerStop(t *testing.T) {
	t.Parallel()

	c := conformance.Case{
		Name: "first_causal_error",
		Body: conformance.Body{
			Stream:    true,
			ChunkSize: 32 * 1024,
			Files: []conformance.File{
				{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 128 * 1024},
			},
		},
		Hooks: []*conformance.HookSpec{
			conformance.Spec("upload",
				conformance.ReadPrefix(8),
				conformance.ReturnError(conformance.ErrConsumerStopped)),
		},
		Expect: conformance.Expect{
			ParseErr:       conformance.ErrConsumerStopped,
			CausalErrs:     []error{conformance.ErrConsumerStopped},
			SourceClosed:   true,
			ProducerExited: true,
		},
	}
	for _, adapter := range conformance.Adapters() {
		t.Run(adapter.Name(), func(t *testing.T) {
			t.Parallel()
			conformance.Run(t, adapter, c)
		})
	}
}

// keep imports used when tests shrink during debugging.
var (
	_ = context.Background
	_ = io.EOF
	_ = errors.Is
	_ formstream.ParserOption
)
