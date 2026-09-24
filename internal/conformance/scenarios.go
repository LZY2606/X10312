package conformance

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/mazrean/formstream"
)

// Scenarios returns the fixed inputs covering the formstream streaming
// contract. They are written once and run against every public adapter.
func Scenarios() []Scenario {
	largeFileSize := int64(8 * int64(formstream.MB))

	return []Scenario{
		{
			Name: "empty-field-and-empty-file-parts",
			Parts: []PartSpec{
				{Name: "note", Data: []byte("")},
				{Name: "upload", Filename: "empty.bin", ContentType: "application/octet-stream", Data: []byte("")},
				{Name: "after", Data: []byte("tail")},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1},
			},
			Expect:          ExpectSuccess,
			ExpectValues:    map[string]string{"note": "", "after": "tail"},
			ExpectFiles:     map[string]map[string]int{"upload": {"empty.bin": 1}},
			ExpectFileBytes: map[string][]byte{"upload": {}},
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "oversized-header-rejected-as-malformed",
			Parts: []PartSpec{
				{
					Name:   "note",
					Data:   []byte("v"),
					Header: map[string]string{"X-Bloat": strings.Repeat("z", 11*1024*1024)},
				},
			},
			Expect:          ExpectMalformed,
			Heavy:           true,
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "header-budget-exhausted-uses-error-hierarchy",
			Parts: []PartSpec{
				{
					Name: "note",
					Data: []byte("v"),
					Header: map[string]string{
						"X-One": "1", "X-Two": "2", "X-Three": "3",
					},
				},
			},
			Options:         Options{MaxHeaders: 2},
			Expect:          ExpectBudgetHeaders,
			ExpectErr:       formstream.ErrTooManyHeaders,
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "unicode-filename-preserved",
			Parts: []PartSpec{
				{Name: "upload", Filename: "看板/画像 データ.png", ContentType: "image/png", Data: []byte("PNG-BYTES")},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1},
			},
			Expect:          ExpectSuccess,
			ExpectFiles:     map[string]map[string]int{"upload": {"看板/画像 データ.png": 1}},
			ExpectFileBytes: map[string][]byte{"upload": []byte("PNG-BYTES")},
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "multiple-files-same-field-all-delivered",
			Parts: []PartSpec{
				{Name: "uploads", Filename: "a.txt", ContentType: "text/plain", Data: []byte("AAA")},
				{Name: "uploads", Filename: "b.txt", ContentType: "text/plain", Data: []byte("BBBB")},
				{Name: "uploads", Filename: "c.txt", ContentType: "text/plain", Data: []byte("CCCCC")},
			},
			Hooks: map[string]HookSpec{
				"uploads": {Limit: -1},
			},
			Expect: ExpectSuccess,
			ExpectFiles: map[string]map[string]int{
				"uploads": {"a.txt": 1, "b.txt": 1, "c.txt": 1},
			},
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "slow-consumer-never-buffers-whole-stream",
			Parts: []PartSpec{
				{Name: "meta", Data: []byte("m")},
				{Name: "upload", Filename: "big.bin", ContentType: "application/octet-stream",
					Data: bytes.Repeat([]byte("x"), int(largeFileSize))},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1, RequiredParts: []string{"meta"}, Delay: slowGate()},
			},
			SlowDelay: slowGate(),
			Options: Options{
				MaxMemSize:     16 * formstream.MB,
				MaxMemFileSize: 1 * formstream.MB,
			},
			Mutation:        BodySlow,
			Expect:          ExpectSuccess,
			ExpectValues:    map[string]string{"meta": "m"},
			ExpectFiles:     map[string]map[string]int{"upload": {"big.bin": 1}},
			MaxPeakInflight: 64 * 1024,
			LargeBodySize:   largeFileSize,
			Heavy:           true,
		},
		{
			Name: "reader-failure-mid-stream-keeps-cause",
			Parts: []PartSpec{
				{Name: "upload", Filename: "half.bin", ContentType: "application/octet-stream",
					Data: bytes.Repeat([]byte("y"), 256*1024)},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1},
			},
			Mutation:        BodyFailingReader,
			Expect:          ExpectReaderFailure,
			ExpectErr:       ErrReaderFailure,
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "consumer-reads-prefix-only-still-parses-rest",
			Parts: []PartSpec{
				{Name: "upload", Filename: "p.bin", ContentType: "application/octet-stream",
					Data: bytes.Repeat([]byte("z"), 128*1024)},
				{Name: "marker", Data: []byte("end-marker")},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: 16},
			},
			Expect:          ExpectSuccess,
			ExpectValues:    map[string]string{"marker": "end-marker"},
			ExpectFiles:     map[string]map[string]int{"upload": {"p.bin": 1}},
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "consumer-error-aborts-and-drains-resources",
			Parts: []PartSpec{
				{Name: "upload", Filename: "e.bin", ContentType: "application/octet-stream",
					Data: bytes.Repeat([]byte("e"), 128*1024)},
				{Name: "later", Data: []byte("never")},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: 4, ErrHook: ErrHookFailure},
			},
			Expect:          ExpectHookFailure,
			ExpectErr:       ErrHookFailure,
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "context-canceled-during-slow-consumer",
			Parts: []PartSpec{
				{Name: "upload", Filename: "c.bin", ContentType: "application/octet-stream",
					Data: bytes.Repeat([]byte("c"), 4*1024*1024)},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1, Delay: blockedGate()},
			},
			CancelWhen:      func(cancel func()) { cancel() },
			Expect:          ExpectContextCanceled,
			ExpectErr:       context.Canceled,
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "recoverable-failure-retry-is-stable",
			Parts: []PartSpec{
				{Name: "note", Data: []byte("stable")},
				{Name: "upload", Filename: "r.bin", ContentType: "application/octet-stream",
					Data: bytes.Repeat([]byte("r"), 64*1024)},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1, RequiredParts: []string{"note"}},
			},
			Expect:          ExpectSuccess,
			ExpectValues:    map[string]string{"note": "stable"},
			ExpectFiles:     map[string]map[string]int{"upload": {"r.bin": 1}},
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "parts-budget-exhausted-uses-error-hierarchy",
			Parts: []PartSpec{
				{Name: "a", Data: []byte("1")},
				{Name: "b", Data: []byte("2")},
				{Name: "c", Data: []byte("3")},
			},
			Options:         Options{MaxParts: 2},
			Expect:          ExpectBudgetParts,
			ExpectErr:       formstream.ErrTooManyParts,
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "memory-budget-exhausted-uses-error-hierarchy",
			Parts: []PartSpec{
				{Name: "bigfield", Data: bytes.Repeat([]byte("m"), 8192)},
			},
			Options: Options{
				MaxMemSize:     64,
				MaxMemFileSize: 64,
			},
			Expect:          ExpectBudgetMemory,
			ExpectErr:       formstream.ErrTooLargeForm,
			MaxPeakInflight: 64 * 1024,
		},
		{
			Name: "large-file-spooled-not-held-in-memory-success",
			Parts: []PartSpec{
				{Name: "meta", Data: []byte("ok")},
				{Name: "upload", Filename: "large.bin", ContentType: "application/octet-stream",
					Data: bytes.Repeat([]byte("L"), int(largeFileSize))},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1, RequiredParts: []string{"meta"}},
			},
			Options: Options{
				MaxMemSize:     2 * formstream.MB,
				MaxMemFileSize: 1 * formstream.MB,
			},
			Expect:          ExpectSuccess,
			ExpectValues:    map[string]string{"meta": "ok"},
			ExpectFiles:     map[string]map[string]int{"upload": {"large.bin": 1}},
			MaxPeakInflight: 64 * 1024,
			LargeBodySize:   largeFileSize,
			Heavy:           true,
		},
		{
			Name: "field-before-and-after-file-both-visible",
			Parts: []PartSpec{
				{Name: "before", Data: []byte("B")},
				{Name: "upload", Filename: "o.bin", ContentType: "application/octet-stream", Data: []byte("O")},
				{Name: "after", Data: []byte("A")},
			},
			Hooks: map[string]HookSpec{
				"upload": {Limit: -1},
			},
			Expect:          ExpectSuccess,
			ExpectValues:    map[string]string{"before": "B", "after": "A"},
			ExpectFiles:     map[string]map[string]int{"upload": {"o.bin": 1}},
			ExpectFileBytes: map[string][]byte{"upload": []byte("O")},
			MaxPeakInflight: 64 * 1024,
		},
	}
}

func slowGate() chan struct{} {
	ch := make(chan struct{})
	go func() {
		time.Sleep(2 * time.Millisecond)
		close(ch)
	}()
	return ch
}

func blockedGate() chan struct{} {
	return make(chan struct{})
}

// RetryScenario is the fixed input reused for the recoverable-failure
// acceptance story: first run fails, identical retry succeeds.
func RetryScenario() Scenario {
	sc := Scenario{
		Name: "recoverable-failure-retry-is-stable",
		Parts: []PartSpec{
			{Name: "note", Data: []byte("stable")},
			{Name: "upload", Filename: "r.bin", ContentType: "application/octet-stream",
				Data: bytes.Repeat([]byte("r"), 64*1024)},
		},
		Hooks: map[string]HookSpec{
			"upload": {Limit: -1, RequiredParts: []string{"note"}},
		},
		Expect:          ExpectSuccess,
		ExpectValues:    map[string]string{"note": "stable"},
		ExpectFiles:     map[string]map[string]int{"upload": {"r.bin": 1}},
		MaxPeakInflight: 64 * 1024,
	}
	return sc
}

var _ = io.EOF
var _ = errors.New
