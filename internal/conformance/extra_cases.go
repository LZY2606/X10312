package conformance

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/mazrean/formstream"
)

var errCancelCause = errors.New("conformance: caller gave up waiting")

func extraCases() []Case {
	return []Case{
		{
			Name: "oversized-header-line-is-accepted-within-budget",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						{
							Name:      "file",
							FileName:  "h.txt",
							MediaType: "text/plain",
							Content:   deterministicBytes(32),
							ExtraHeaders: map[string]string{
								"X-Long": strings.Repeat("z", 20000),
							},
						},
					},
					hooks: []fileHook{{name: "file"}},
					options: []formstream.ParserOption{
						formstream.WithMaxHeaders(10),
					},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "file", FileName: "h.txt",
					ContentType: "text/plain", Size: 32,
					SHA256: shaOfContent(deterministicBytes(32)),
				}})
			},
		},
		{
			Name: "header-budget-exhausted-keeps-sentinel",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						{
							Name:      "file",
							FileName:  "h.txt",
							MediaType: "text/plain",
							Content:   deterministicBytes(16),
						},
					},
					hooks: []fileHook{{name: "file"}},
					options: []formstream.ParserOption{
						formstream.WithMaxHeaders(2),
					},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, formstream.ErrTooManyHeaders)
				if len(res.Files) != 0 {
					t.Errorf("%s hook must not run after budget failure; observed %s", label, describeObs(res.Files))
				}
			},
		},
		{
			Name: "part-budget-exhausted-keeps-sentinel",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("a", "1"),
						FieldPart("b", "2"),
					},
					options: []formstream.ParserOption{formstream.WithMaxParts(1)},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, formstream.ErrTooManyParts)
			},
		},
		{
			Name: "memory-budget-exhausted-keeps-sentinel",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{FieldPart("big", strings.Repeat("z", 128))},
					options: []formstream.ParserOption{
						formstream.WithMaxMemSize(16),
						formstream.WithMaxMemFileSize(16),
					},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, formstream.ErrTooLargeForm)
			},
		},
		{
			Name:       "reader-fails-mid-file-fast-path-first-cause-is-kept",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts:      []PartSpec{FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize)},
					hooks:      []fileHook{{name: "file"}},
					streamOpts: StreamOptions{FailAfter: 2 * chunkSize},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, ErrInjectedReaderFailure)
				if len(res.Files) != 1 || res.Files[0].ReadError == nil ||
					!errors.Is(res.Files[0].ReadError, ErrInjectedReaderFailure) {
					t.Errorf("%s consumer must observe the injected reader cause; observed %s", label, describeObs(res.Files))
				}
			},
		},
		{
			Name:       "reader-fails-mid-file-slow-path-first-cause-is-kept",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("key", "v"),
						FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize),
					},
					hooks:      []fileHook{{name: "file", requireParts: []string{"key"}}},
					streamOpts: StreamOptions{FailAfter: 2 * chunkSize},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, ErrInjectedReaderFailure)
			},
		},
		{
			Name:       "consumer-prefix-only-fast-path-unblocks-producer",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize)},
					hooks: []fileHook{{name: "file", prefixOnly: true}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, ErrConsumerPrefixOnly)
				if len(res.Files) != 1 || res.Files[0].Size != 2*chunkSize {
					t.Errorf("%s consumer should read exactly %d prefix bytes; observed %s", label, 2*chunkSize, describeObs(res.Files))
				}
			},
		},
		{
			Name:       "consumer-prefix-only-slow-path-unblocks-producer",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("key", "v"),
						FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize),
					},
					hooks: []fileHook{{name: "file", requireParts: []string{"key"}, prefixOnly: true}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, ErrConsumerPrefixOnly)
			},
		},
		{
			Name:       "context-cancel-fast-path-keeps-caller-cause",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts:       []PartSpec{FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize)},
					hooks:       []fileHook{{name: "file", cancelOnData: true}},
					cancelCause: errCancelCause,
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, errCancelCause)
				if !errors.Is(res.Err, context.Canceled) {
					t.Errorf("%s caller cancel cause should remain a context cancellation; got %v", label, res.Err)
				}
			},
		},
		{
			Name:       "context-cancel-slow-path-keeps-caller-cause",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("key", "v"),
						FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize),
					},
					hooks:       []fileHook{{name: "file", requireParts: []string{"key"}, cancelOnData: true}},
					cancelCause: errCancelCause,
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, errCancelCause)
			},
		},
		{
			Name:       "context-deadline-slow-consumer-keeps-deadline-exceeded",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("key", "v"),
						FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize),
					},
					hooks:    []fileHook{{name: "file", requireParts: []string{"key"}, slow: true}},
					deadline: 250 * time.Millisecond,
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, context.DeadlineExceeded)
			},
		},
		{
			Name:       "truncated-body-inside-file-is-corrupt-input",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts:      []PartSpec{FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize)},
					hooks:      []fileHook{{name: "file"}},
					streamOpts: StreamOptions{Truncate: true},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				if res.Err == nil {
					t.Errorf("%s truncated file body must not be reported as success; got nil", label)
					return
				}
				if !errors.Is(res.Err, io.ErrUnexpectedEOF) && !errors.Is(res.Err, io.EOF) {
					t.Errorf("%s truncated body error should preserve EOF/ErrUnexpectedEOF; got %v", label, res.Err)
				}
			},
		},
		{
			Name:       "duplicate-hook-name-is-rejected-repeatably",
			Deliveries: streamOnly(),
			EntryOnly: func(t TestingT, a Adapter) {
				noop := func(_ io.Reader, _ formstream.Header) error { return nil }
				first := a.NewParser(t, NewMemory([]PartSpec{FieldPart("a", "1")}), ContentType)
				if err := first.Register("dup", noop); err != nil {
					t.Fatalf("[%s] first Register should succeed; got %v", a.Name, err)
				}
				err2 := first.Register("dup", noop)
				var dup formstream.DuplicateHookNameError
				if !errors.As(err2, &dup) || dup.Name != "dup" {
					t.Fatalf("[%s] duplicate hook error should be formstream.DuplicateHookNameError{dup}; got %v", a.Name, err2)
				}
				second := a.NewParser(t, NewMemory([]PartSpec{FieldPart("a", "1")}), ContentType)
				err3 := second.Register("dup", noop)
				var dup2 formstream.DuplicateHookNameError
				if !errors.As(err3, &dup2) {
					t.Fatalf("[%s] duplicate rejection must be repeatable on a fresh parser; got %v", a.Name, err3)
				}
			},
		},
		{
			Name:       "adapter-entry-rejects-wrong-content-type-and-missing-boundary",
			Deliveries: streamOnly(),
			EntryOnly: func(t TestingT, a Adapter) {
				if a.InvalidContentTypeError != nil {
					parser, err := newAdapterParserError(a, "text/plain")
					if !errors.Is(err, a.InvalidContentTypeError) {
						t.Errorf("[%s] wrong Content-Type should return %v; got parser=%v err=%v", a.Name, a.InvalidContentTypeError, parser != nil, err)
					}
				}
				if a.MissingBoundaryError != nil {
					parser, err := newAdapterParserError(a, "multipart/form-data")
					if !errors.Is(err, a.MissingBoundaryError) {
						t.Errorf("[%s] missing boundary should return %v; got parser=%v err=%v", a.Name, a.MissingBoundaryError, parser != nil, err)
					}
				}
			},
		},
	}
}
