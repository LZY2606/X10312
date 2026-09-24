package conformance

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/mazrean/formstream"
)

// Sentinel errors owned by the caller side of the contract.
var (
	ErrConsumerFailed  = errors.New("conformance: consumer failed")
	ErrReaderDied      = errors.New("conformance: upstream reader died")
	ErrConsumerStopped = errors.New("conformance: consumer stopped after prefix")
)

// largeFileSize is big enough to force the slow path to disk on every
// platform while remaining cheap (the harness itself never materializes it).
var (
	largeFileSize = 4 << 20
	streamChunk   = 64 * 1024
)

// FixedCases are the stable counter-example inputs. Names identify the
// contract they break under a naive implementation.
func FixedCases() []Case {
	cases := []Case{
		// 1. empty part: zero-length field and zero-length file.
		{
			Name: "empty_part_field_and_file",
			Body: Body{
				Fields: []Field{{Name: "empty", Value: ""}},
				Files: []File{
					{Name: "upload", Filename: "empty.bin", MediaType: "application/octet-stream", Size: 0},
				},
			},
			Hooks: []*HookSpec{
				Spec("upload", CheckContent(), ExpectBytes(0),
					ExpectFilename("empty.bin"), ExpectContentType("application/octet-stream")),
			},
			Expect: Expect{
				HookMinCalls:   map[string]int{"upload": 1},
				Values:         map[string]string{"empty": ""},
				ProducerExited: false,
			},
		},
		// 2. overlong header: many distinct header entries exceed the budget.
		{
			Name: "overlong_header_budget_exhaustion",
			Body: Body{
				Files: []File{{
					Name:         "upload",
					Filename:     "a.bin",
					MediaType:    "application/octet-stream",
					Size:         4,
					ExtraHeaders: overlongHeaders(16),
				}},
			},
			Hooks: []*HookSpec{Spec("upload")},
			Opts: []formstream.ParserOption{
				formstream.WithMaxHeaders(8),
				formstream.WithMaxParts(10),
			},
			Expect: Expect{
				ParseErr:       formstream.ErrTooManyHeaders,
				SourceClosed:   true,
				ProducerExited: false,
			},
		},
		// 3. Unicode filename, both raw UTF-8 (browser style) and RFC 5987.
		unicodeFilenameCase(),
		// 4. multiple files under one field name: every file delivered in order.
		{
			Name: "multiple_files_same_field",
			Body: Body{
				Files: []File{
					{Name: "uploads", Filename: "f1.bin", MediaType: "application/octet-stream", Size: 1000, Content: func(i int) byte { return byte(i) }},
					{Name: "uploads", Filename: "f2.bin", MediaType: "application/octet-stream", Size: 2500},
					{Name: "uploads", Filename: "f3.bin", MediaType: "application/octet-stream", Size: 7, Content: func(i int) byte { return byte(255 - i) }},
				},
			},
			Hooks: []*HookSpec{
				Spec("uploads", CheckContent(), ExpectFilename("f1.bin")),
			},
			Expect: Expect{
				HookMinCalls: map[string]int{"uploads": 3},
			},
		},
		// 5. slow consumer: the producer must keep streaming bounded chunks and
		// exit cleanly; the parser must not buffer the whole stream ahead.
		{
			Name: "slow_consumer_streaming_footprint",
			Body: Body{
				Stream:    true,
				ChunkSize: streamChunk,
				Files: []File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 256 * 1024},
				},
			},
			Hooks: []*HookSpec{Spec("upload", CheckContent(), slowConsumer())},
			Expect: Expect{
				HookMinCalls:    map[string]int{"upload": 1},
				MaxPeakInFlight: int64(streamChunk * 4),
				ProducerExited:  true,
			},
		},
		// 6a. reader dies mid-stream while a hook drains the fast path: first
		// causal error is the upstream failure; source is released.
		{
			Name: "reader_mid_failure_fastpath",
			Body: Body{
				Stream:    true,
				ChunkSize: streamChunk,
				FailAfter: 40 * 1024,
				FailErr:   ErrReaderDied,
				Files: []File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 128 * 1024},
				},
			},
			Hooks: []*HookSpec{Spec("upload")},
			Expect: Expect{
				CausalErrs:     []error{ErrReaderDied},
				SourceClosed:   true,
				ProducerExited: true,
			},
		},
		// 6b. reader dies mid-stream on the slow path (required field follows):
		// spooled pending state must still be released and the cause preserved.
		{
			Name: "reader_mid_failure_slowpath_pending_release",
			Body: Body{
				Stream:    true,
				ChunkSize: streamChunk,
				FailAfter: 32 * 1024,
				FailErr:   ErrReaderDied,
				Files: []File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 128 * 1024},
				},
			},
			Hooks: []*HookSpec{Spec("upload", WithRequired("later"))},
			Expect: Expect{
				CausalErrs:   []error{ErrReaderDied},
				SourceClosed: true,
			},
		},
		// 7a. consumer reads only a prefix and succeeds: remaining part is
		// drained through the shared parser path and the producer still exits.
		{
			Name: "consumer_prefix_only_success",
			Body: Body{
				Stream:    true,
				ChunkSize: streamChunk,
				Files: []File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 128 * 1024},
				},
			},
			Hooks: []*HookSpec{
				Spec("upload", ReadPrefix(16), ExpectBytes(16)),
			},
			Expect: Expect{
				HookMinCalls:   map[string]int{"upload": 1},
				ProducerExited: true,
			},
		},
		// 7b. consumer reads a prefix then returns an error: the producer
		// goroutine writing the remainder must be unblocked and exit; the
		// consumer error is the first causal error (a naive implementation
		// leaks the producer here).
		{
			Name: "consumer_prefix_error_unblocks_producer",
			Body: Body{
				Stream:    true,
				ChunkSize: streamChunk,
				Files: []File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: 128 * 1024},
				},
			},
			Hooks: []*HookSpec{
				Spec("upload", ReadPrefix(16), ExpectBytes(16), ReturnError(ErrConsumerStopped)),
			},
			Expect: Expect{
				ParseErr:       ErrConsumerStopped,
				CausalErrs:     []error{ErrConsumerStopped},
				SourceClosed:   true,
				ProducerExited: true,
			},
		},
		// 8. context canceled while the producer is slow: cancellation is the
		// reported cause and every goroutine exits.
		{
			Name: "context_canceled_abort_releases_producer",
			Body: Body{
				Stream:    true,
				ChunkSize: streamChunk,
				Files: []File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: largeFileSize},
				},
			},
			Hooks: []*HookSpec{Spec("upload", blockUntilContextCanceled())},
			// cancel after a few throttled reads, while streaming is active.
			CancelAfter: 30 * time.Millisecond,
			Expect: Expect{
				ParseErr:       context.Canceled,
				CausalErrs:     []error{context.Canceled},
				SourceClosed:   true,
				ProducerExited: true,
			},
		},
		// 9. large file on the slow path is parsed without holding the whole
		// file ahead of the consumer and spills to a released temp file.
		{
			Name: "large_file_slowpath_no_whole_file_buffered",
			Body: Body{
				Stream:    true,
				ChunkSize: streamChunk,
				Files: []File{
					{Name: "upload", Filename: "big.bin", MediaType: "application/octet-stream", Size: largeFileSize},
				},
				Fields: []Field{{Name: "after", Value: "done"}},
			},
			Hooks: []*HookSpec{
				// upload arrives before "after", forcing the spool path.
				Spec("upload", WithRequired("after"), CheckContent(), slowConsumer()),
			},
			Opts: []formstream.ParserOption{
				formstream.WithMaxMemFileSize(256 * formstream.KB),
			},
			Expect: Expect{
				HookMinCalls:    map[string]int{"upload": 1},
				Values:          map[string]string{"after": "done"},
				MaxPeakInFlight: int64(streamChunk * 4),
				DiskSpool:       true,
				ProducerExited:  true,
			},
		},
		// 10. corrupt input: truncated body surfaces a parse error, not a hang.
		{
			Name: "corrupt_truncated_body",
			Body: Body{
				Truncate: true,
				Files: []File{
					{Name: "upload", Filename: "x.bin", MediaType: "application/octet-stream", Size: 32},
				},
			},
			Hooks: []*HookSpec{Spec("upload")},
			Expect: Expect{
				ParseErr:     io.ErrUnexpectedEOF,
				CausalErrs:   []error{io.ErrUnexpectedEOF},
				SourceClosed: true,
			},
		},
		// 11. memory budget exhausted by a regular field: existing error tier.
		{
			Name: "memory_budget_exhausted",
			Body: Body{
				Fields: []Field{{Name: "big", Value: strings.Repeat("x", 4096)}},
			},
			Hooks: nil,
			Opts:  []formstream.ParserOption{formstream.WithMaxMemSize(1 * formstream.KB)},
			Expect: Expect{
				ParseErr: formstream.ErrTooLargeForm,
			},
		},
	}
	return cases
}

// overlongHeaders builds many distinct header entries so the header-entry
// budget is exhausted deterministically (a single huge value is capped by
// mime/multipart's own 10MiB header-size limit instead).
func overlongHeaders(n int) map[string]string {
	h := map[string]string{}
	for i := 0; i < n; i++ {
		h["X-Pad-"+strconv.Itoa(i)] = "v"
	}
	return h
}
