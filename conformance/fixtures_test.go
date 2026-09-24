package conformance_test

import (
	"time"

	"github.com/mazrean/formstream"
)

// unicodeFileName exercises RFC 2231 filename encoding end to end.
const unicodeFileName = "レポート-🚀-2026.txt"

// conformanceFixtures is the fixed negative/edge corpus. Every case is
// small, deterministic, and enters only through the public adapters.
func conformanceFixtures() []fixture {
	slowFile := patternBytes(512 * 1024)
	prefixFile := patternBytes(256 * 1024)
	failFile := patternBytes(256 * 1024)

	return []fixture{
		{
			name: "empty_part",
			parts: []partSpec{
				{name: "empty", content: []byte{}},
				{name: "note", content: []byte("hello")},
			},
			want: canonicalResult{
				Values: map[string][]string{
					"empty": {""},
					"note":  {"hello"},
				},
				Files: []fileEvent{},
			},
		},
		{
			name: "oversized_header",
			parts: []partSpec{
				{
					name:    "meta",
					content: []byte("x"),
					extraHeaders: [][2]string{
						{"X-Custom-1", "1"},
						{"X-Custom-2", "2"},
						{"X-Custom-3", "3"},
						{"X-Custom-4", "4"},
						{"X-Custom-5", "5"},
						{"X-Custom-6", "6"},
					},
				},
			},
			parserOpts: []formstream.ParserOption{formstream.WithMaxHeaders(4)},
			wantErr:    formstream.ErrTooManyHeaders,
		},
		{
			name: "unicode_filename",
			parts: []partSpec{
				{name: "upload", fileName: unicodeFileName, contentType: "text/plain", content: []byte("unicode body")},
			},
			hooks: []hookSpec{{name: "upload", kind: consumeDrain}},
			want: canonicalResult{
				Values: map[string][]string{},
				Files: []fileEvent{{
					Name:        "upload",
					FileName:    unicodeFileName,
					ContentType: "text/plain",
					Size:        int64(len("unicode body")),
					SHA256:      sha256Hex([]byte("unicode body")),
				}},
			},
		},
		{
			name: "multi_file_same_field",
			parts: []partSpec{
				{name: "file", fileName: "a.txt", contentType: "text/plain", content: []byte("AAA")},
				{name: "file", fileName: "b.txt", contentType: "text/plain", content: []byte("BBB")},
				{name: "tag", content: []byte("v1")},
				{name: "tag", content: []byte("v2")},
			},
			hooks: []hookSpec{{name: "file", kind: consumeDrain}},
			want: canonicalResult{
				Values: map[string][]string{"tag": {"v1", "v2"}},
				Files: []fileEvent{
					{Name: "file", FileName: "a.txt", ContentType: "text/plain", Size: 3, SHA256: sha256Hex([]byte("AAA"))},
					{Name: "file", FileName: "b.txt", ContentType: "text/plain", Size: 3, SHA256: sha256Hex([]byte("BBB"))},
				},
			},
		},
		{
			name: "slow_consumer",
			parts: []partSpec{
				{name: "payload", fileName: "payload.bin", contentType: "application/octet-stream", content: slowFile},
			},
			hooks: []hookSpec{{name: "payload", kind: consumeDrain, slowPerRead: time.Millisecond}},
			want: canonicalResult{
				Values: map[string][]string{},
				Files: []fileEvent{{
					Name:        "payload",
					FileName:    "payload.bin",
					ContentType: "application/octet-stream",
					Size:        int64(len(slowFile)),
					SHA256:      sha256Hex(slowFile),
				}},
			},
		},
		{
			name: "reader_mid_failure",
			parts: []partSpec{
				{name: "payload", fileName: "payload.bin", contentType: "application/octet-stream", content: failFile},
				{name: "tail", content: []byte("unreachable")},
			},
			hooks:        []hookSpec{{name: "payload", kind: consumeDrain}},
			corruptAfter: 100 * 1024,
			wantErr:      errBoom,
		},
		{
			name: "consumer_prefix_only",
			parts: []partSpec{
				{name: "payload", fileName: "payload.bin", contentType: "application/octet-stream", content: prefixFile},
				{name: "after", content: []byte("done")},
			},
			hooks: []hookSpec{{name: "payload", kind: consumePrefix, limit: 128}},
			want: canonicalResult{
				Values: map[string][]string{"after": {"done"}},
				Files: []fileEvent{{
					Name:        "payload",
					FileName:    "payload.bin",
					ContentType: "application/octet-stream",
					Size:        128,
					SHA256:      sha256Hex(prefixFile[:128]),
				}},
			},
		},
	}
}
