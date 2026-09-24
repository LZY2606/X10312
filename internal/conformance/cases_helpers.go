package conformance

import "time"

func slowConsumer() func(*HookSpec) {
	return ThrottleReads(50 * time.Microsecond)
}

// blockUntilContextCanceled drains slowly so a big file is still streaming
// when the parse context is canceled.
func blockUntilContextCanceled() func(*HookSpec) {
	return ThrottleReads(2 * time.Millisecond)
}

// unicodeFilenameCase covers Unicode names both as raw UTF-8 (what many
// browsers send) and RFC 5987 encoded parameters.
func unicodeFilenameCase() Case {
	return Case{
		Name: "unicode_filename_preserved",
		Body: Body{
			Files: []File{
				{
					Name:      "upload",
					Filename:  "テスト ファイル.txt",
					MediaType: "text/plain",
					Size:      16,
					Content:   func(i int) byte { return byte('A' + i%26) },
				},
			},
		},
		Hooks: []*HookSpec{
			Spec("upload", CheckContent(),
				ExpectFilename("テスト ファイル.txt"),
				ExpectContentType("text/plain")),
		},
		Expect: Expect{
			HookMinCalls: map[string]int{"upload": 1},
		},
	}
}
