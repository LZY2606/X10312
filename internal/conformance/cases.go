package conformance

import (
	"crypto/sha256"
	"fmt"
)

// Case is one conformance scenario.
type Case struct {
	// Name identifies the contract the case checks.
	Name string
	// Deliveries restricts how the body is delivered; defaults to both.
	Deliveries []Delivery
	// Setup builds the run configuration.
	Setup func() runConfig
	// Expect validates case-specific observable semantics.
	Expect func(t TestingT, label string, res Result)
	// EntryOnly is run against the adapter constructor instead of Parse.
	EntryOnly func(t TestingT, a Adapter)
}

func bothDeliveries() []Delivery {
	return []Delivery{DeliveryStream, DeliveryMemory}
}

func streamOnly() []Delivery { return []Delivery{DeliveryStream} }

func shaOfContent(content []byte) string {
	sum := sha256.Sum256(content)
	return fmt.Sprintf("%x", sum)
}

// Run executes every case against the adapter through the applicable
// deliveries.
func Run(t TestingT, a Adapter) {
	t.Helper()
	for _, tc := range cases() {
		tc := tc
		t.Run(tc.Name, func(t TestingT) {
			if tc.EntryOnly != nil {
				tc.EntryOnly(t, a)
				return
			}
			ds := tc.Deliveries
			if len(ds) == 0 {
				ds = bothDeliveries()
			}
			for _, d := range ds {
				d := d
				t.Run(string(d), func(t TestingT) {
					cfg := tc.Setup()
					res := runOnce(t, a, d, cfg)
					if tc.Expect != nil {
						tc.Expect(t, fmt.Sprintf("[%s/%s/%s]", a.Name, tc.Name, d), res)
					}
				})
			}
		})
	}
}

const conformanceFileSize = 256 * 1024

func successFieldFileCase() runConfig {
	return runConfig{
		parts: []PartSpec{
			FieldPart("title", "hello"),
			FilePart("file", "a.txt", "text/plain", conformanceFileSize),
		},
		hooks: []fileHook{{name: "file"}},
	}
}

func cases() []Case {
	return append([]Case{
		{
			Name: "empty-parts-are-preserved",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("empty", ""),
						FilePart("avatar", "empty.bin", "application/octet-stream", 0),
						FieldPart("nonempty", "v"),
					},
					hooks: []fileHook{{name: "avatar"}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFields(t, label, res, map[string]string{"empty": "", "nonempty": "v"})
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "avatar", FileName: "empty.bin",
					ContentType: "application/octet-stream", Size: 0, SHA256: shaOfContent(nil),
				}})
			},
		},
		{
			Name: "unicode-filename-roundtrips",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("title", "t"),
						FilePart("file", "テスト-🚀.txt", "text/plain", 128),
					},
					hooks: []fileHook{{name: "file"}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFiles(t, label, res, []fileExpectation{{
					HookName: "file", FileName: "テスト-🚀.txt",
					ContentType: "text/plain", Size: 128,
					SHA256: shaOfContent(deterministicBytes(128)),
				}})
			},
		},
		{
			Name: "multiple-files-same-field-are-observed-in-order",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FilePart("uploads", "1.txt", "text/plain", 100),
						FilePart("uploads", "2.txt", "text/plain", 200),
						FilePart("uploads", "3.txt", "text/plain", 50),
					},
					hooks: []fileHook{{name: "uploads"}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFiles(t, label, res, []fileExpectation{
					{HookName: "uploads", FileName: "1.txt", ContentType: "text/plain", Size: 100, SHA256: shaOfContent(deterministicBytes(100))},
					{HookName: "uploads", FileName: "2.txt", ContentType: "text/plain", Size: 200, SHA256: shaOfContent(deterministicBytes(200))},
					{HookName: "uploads", FileName: "3.txt", ContentType: "text/plain", Size: 50, SHA256: shaOfContent(deterministicBytes(50))},
				})
			},
		},
		{
			Name: "slow-path-multiple-files-same-field-are-observed-in-order",
			Setup: func() runConfig {
				return runConfig{
					parts: []PartSpec{
						FieldPart("key", "k"),
						FilePart("uploads", "1.txt", "text/plain", 100),
						FilePart("uploads", "2.txt", "text/plain", 200),
						FilePart("uploads", "3.txt", "text/plain", 50),
					},
					hooks: []fileHook{{name: "uploads", requireParts: []string{"key"}}},
				}
			},
			Expect: func(t TestingT, label string, res Result) {
				expectError(t, label, res, nil)
				expectFields(t, label, res, map[string]string{"key": "k"})
				expectFiles(t, label, res, []fileExpectation{
					{HookName: "uploads", FileName: "1.txt", ContentType: "text/plain", Size: 100, SHA256: shaOfContent(deterministicBytes(100))},
					{HookName: "uploads", FileName: "2.txt", ContentType: "text/plain", Size: 200, SHA256: shaOfContent(deterministicBytes(200))},
					{HookName: "uploads", FileName: "3.txt", ContentType: "text/plain", Size: 50, SHA256: shaOfContent(deterministicBytes(50))},
				})
			},
		},
	}, append(extraCases(), extraCases2()...)...)
}
