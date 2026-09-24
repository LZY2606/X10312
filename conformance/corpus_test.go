package conformance_test

import (
	"fmt"
	"strings"

	"github.com/mazrean/formstream/internal/conformance"
)

const (
	smallFile = int64(4096)
	largeFile = int64(8) << 20 // 8MiB: larger than any peak gate
)

// basicSuccessForm: field + empty part + unicode-named file.
func basicSuccessForm() conformance.FormSpec {
	return conformance.FormSpec{Parts: []conformance.PartSpec{
		{Field: "field", IsField: true, Value: "mazrean"},
		{Field: "empty", FileName: "empty.bin", ContentType: "application/octet-stream", Size: 0},
		{Field: "avatar", FileName: "テスト ファイル.png", ContentType: "image/png", Size: smallFile},
	}}
}

// multiFileForm: two files under the same field, plus a trailing field.
func multiFileForm() conformance.FormSpec {
	return conformance.FormSpec{Parts: []conformance.PartSpec{
		{Field: "docs", FileName: "a.txt", ContentType: "text/plain", Size: 1024},
		{Field: "docs", FileName: "b.txt", ContentType: "text/plain", Size: 2048},
		{Field: "meta", IsField: true, Value: "meta-value"},
	}}
}

// reversedForm exercises the abnormal (spool) path: the required field
// arrives after the file.
func reversedForm() conformance.FormSpec {
	return conformance.FormSpec{Parts: []conformance.PartSpec{
		{Field: "upload", FileName: "big.bin", ContentType: "application/octet-stream", Size: largeFile},
		{Field: "field", IsField: true, Value: "later"},
	}}
}

// emptyPartsForm: empty field and empty file, both in one stream.
func emptyPartsForm() conformance.FormSpec {
	return conformance.FormSpec{Parts: []conformance.PartSpec{
		{Field: "emptyfield", IsField: true, Value: ""},
		{Field: "emptyfile", FileName: "", ContentType: "text/plain", Size: 0},
	}}
}

// tooManyHeadersForm: one part with more header lines than the budget.
func tooManyHeadersForm(n int) conformance.FormSpec {
	var b strings.Builder
	b.WriteString("--" + conformance.Boundary() + "\r\n")
	b.WriteString(`Content-Disposition: form-data; name="dense"` + "\r\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "X-Pad-%05d: v\r\n", i)
	}
	b.WriteString("\r\nbody\r\n--" + conformance.Boundary() + "--\r\n")
	return conformance.FormSpec{RawForm: b.String()}
}

// hugeSingleHeaderForm: one header value longer than textproto's
// per-line limit; this is input corruption, not a budget condition.
func hugeSingleHeaderForm() conformance.FormSpec {
	var b strings.Builder
	b.WriteString("--" + conformance.Boundary() + "\r\n")
	b.WriteString(`Content-Disposition: form-data; name="dense"` + "\r\n")
	b.WriteString("X-Huge: " + strings.Repeat("z", 2<<20) + "\r\n\r\n")
	b.WriteString("body\r\n--" + conformance.Boundary() + "--\r\n")
	return conformance.FormSpec{RawForm: b.String()}
}

// malformedForm: a part body with no closing boundary.
func malformedForm() conformance.FormSpec {
	return conformance.FormSpec{RawForm: "--" + conformance.Boundary() + "\r\n" +
		`Content-Disposition: form-data; name="field"` + "\r\n\r\n" +
		"value-without-closing-boundary"}
}

// hugeFieldForm: a plain field larger than the memory budget.
func hugeFieldForm(size int) conformance.FormSpec {
	return conformance.FormSpec{Parts: []conformance.PartSpec{
		{Field: "bigfield", IsField: true, Value: strings.Repeat("q", size)},
	}}
}

// manyPartsForm: more parts than the parts budget.
func manyPartsForm(n int) conformance.FormSpec {
	parts := make([]conformance.PartSpec, 0, n)
	for i := 0; i < n; i++ {
		parts = append(parts, conformance.PartSpec{
			Field: fmt.Sprintf("p%d", i), IsField: true, Value: "v",
		})
	}
	return conformance.FormSpec{Parts: parts}
}
