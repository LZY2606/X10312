package conformance

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mazrean/formstream"
)

const (
	streamingGateFileSize = 8 * 1024 * 1024
	spillFileSize         = 128 * 1024
	spillBudget           = formstream.DataSize(64 * 1024)
)

func init() {
	// keep context imported for cases exercising cancellation recovery
	_ = context.Background
}

func extraCases2() []Case {
	return []Case{
		{
			Name: "fields-files-and-custom-readers-parse-identically-fast-path",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("title", "report"),
						FieldPart("note", "n"),
						FilePart("file", "report.txt", "text/plain", conformanceFileSize),
					},
					hooks: []fileHook{{name: "file"}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFields(t, label, res, map[string]string{"title": "report", "note": "n"})
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "file", FileName: "report.txt", ContentType: "text/plain",
					Size: conformanceFileSize, SHA256: shaOfContent(deterministicBytes(conformanceFileSize)),
				}})
			},
		},
		{
			Name: "fields-files-and-custom-readers-parse-identically-slow-path",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FilePart("file", "report.txt", "text/plain", conformanceFileSize),
						FieldPart("title", "report"),
						FieldPart("note", "n"),
					},
					hooks: []fileHook{{name: "file", requireParts: []string{"title", "note"}}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFields(t, label, res, map[string]string{"title": "report", "note": "n"})
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "file", FileName: "report.txt", ContentType: "text/plain",
					Size: conformanceFileSize, SHA256: shaOfContent(deterministicBytes(conformanceFileSize)),
				}})
			},
		},
		{
			Name:       "slow-consumer-stays-streaming-and-returns-content",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FilePart("file", "slow.bin", "application/octet-stream", conformanceFileSize),
					},
					hooks: []fileHook{{name: "file", slow: true}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "file", FileName: "slow.bin", ContentType: "application/octet-stream",
					Size: conformanceFileSize, SHA256: shaOfContent(deterministicBytes(conformanceFileSize)),
				}})
			},
		},
		{
			Name:       "large-stream-invokes-hook-before-whole-file-is-delivered",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FilePart("file", "large.bin", "application/octet-stream", streamingGateFileSize),
					},
					hooks:      []fileHook{{name: "file"}},
					streamOpts: StreamOptions{Gate: true},
					gate:       true,
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "file", FileName: "large.bin", ContentType: "application/octet-stream",
					Size: streamingGateFileSize, SHA256: shaOfContent(deterministicBytes(streamingGateFileSize)),
				}})
				// A whole-file buffer implementation would have deadlocked the
				// gate (test timeout) or delivered the file in one Read; both
				// are ruled out by runOnce's shared invariants.
			},
		},
		{
			Name:       "large-slow-path-spills-to-disk-and-releases-resources",
			Deliveries: streamOnly(),
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("key", "v"),
						FilePart("file", "spill.bin", "application/octet-stream", spillFileSize),
					},
					hooks: []fileHook{{name: "file", requireParts: []string{"key"}, slow: true}},
					options: []formstream.ParserOption{
						formstream.WithMaxMemFileSize(spillBudget),
						formstream.WithMaxMemSize(spillBudget),
					},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "file", FileName: "spill.bin", ContentType: "application/octet-stream",
					Size: spillFileSize, SHA256: shaOfContent(deterministicBytes(spillFileSize)),
				}})
			},
		},
		{
			Name:       "retry-after-recoverable-mid-stream-failure-is-stable-and-stateless",
			Deliveries: streamOnly(),
			EntryOnly: func(t TestingT, a Adapter) {
				recoveryStory(t, a)
			},
		},
		{
			Name: "result-is-independent-of-enumeration-order-and-scheduling",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("a", "1"),
						FilePart("file", "x.txt", "text/plain", 4096),
						FieldPart("b", "2"),
						FilePart("other", "y.txt", "text/plain", 2048),
					},
					hooks: []fileHook{
						{name: "file"},
						{name: "other", slow: true},
					},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFields(t, label, res, map[string]string{"a": "1", "b": "2"})
				expectFiles(t, label, res, []fileExpectation{
					{HookName: "file", FileName: "x.txt", ContentType: "text/plain", Size: 4096, SHA256: shaOfContent(deterministicBytes(4096))},
					{HookName: "other", FileName: "y.txt", ContentType: "text/plain", Size: 2048, SHA256: shaOfContent(deterministicBytes(2048))},
				})
			},
		},
	}
}

// recoveryStory drives the multi-stage acceptance story: first a hook aborts
// after seeing a prefix (recoverable failure), then the exact same form is
// retried on a fresh parser and succeeds. A failed attempt must leave no cached
// bytes/counters/temp state behind for the success.
func recoveryStory(t TestingT, a Adapter) {
	t.Helper()
	parts := []PartSpec{FilePart("file", "f.bin", "application/octet-stream", conformanceFileSize)}

	first := runOnce(t, a, DeliveryStream, runConfig{
		parts: parts,
		hooks: []fileHook{{name: "file", prefixOnly: true}},
	})
	if !errors.Is(first.Err, ErrConsumerPrefixOnly) {
		t.Fatalf("[%s/recovery] first attempt should fail with the prefix abort; got %v", a.Name, first.Err)
	}

	second := runOnce(t, a, DeliveryStream, runConfig{
		parts: parts,
		hooks: []fileHook{{name: "file"}},
	})
	expectError(t, fmt.Sprintf("[%s/recovery/retry]", a.Name), second, nil)
	expectFiles(t, fmt.Sprintf("[%s/recovery/retry]", a.Name), second, []fileExpectation{{
		HookName: "file", FileName: "f.bin", ContentType: "application/octet-stream",
		Size: conformanceFileSize, SHA256: shaOfContent(deterministicBytes(conformanceFileSize)),
	}})
	if len(second.Fields) != 0 {
		t.Errorf("[%s/recovery/retry] success must not carry values from the failed attempt; got %d field keys: %s",
			a.Name, len(second.Fields), strings.Join(fieldKeys(second.Fields), ","))
	}
}

func fieldKeys(m map[string][]formstream.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
