package conformance

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
)

// Boundary is the fixed boundary of conformance bodies.
const Boundary = "conformance-boundary"

// Field is a regular form field part.
type Field struct {
	Name  string
	Value string
}

// File is a file part. Content(offset) determines each byte so test code
// never holds the whole large file in memory.
type File struct {
	Name      string
	Filename  string
	MediaType string
	Size      int
	Content   func(offset int) byte
	// RawContentDisposition, when set, replaces the generated disposition.
	RawContentDisposition string
	// ExtraHeaders adds arbitrary header lines (overlong-header case).
	ExtraHeaders map[string]string
}

// Body is a scripted multipart body.
type Body struct {
	Fields []Field
	Files  []File

	// Stream serves via a chunk-pumped channel (models an HTTP/pipe producer).
	Stream bool
	// ChunkSize bounds each producer write.
	ChunkSize int
	// FailAfter > 0 makes the source return FailErr after that many bytes.
	FailAfter int
	FailErr   error
	// Truncate omits the final closing delimiter (corrupt body).
	Truncate bool
}

func deterministic(offset int) byte { return byte(offset*31 + 7) }

// Build renders the body. Large files are generated in bounded pieces.
func (b Body) Build() []byte {
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	if err := mw.SetBoundary(Boundary); err != nil {
		panic(err)
	}

	for _, f := range b.Fields {
		if err := mw.WriteField(f.Name, f.Value); err != nil {
			panic(err)
		}
	}
	for _, f := range b.Files {
		h := textproto.MIMEHeader{}
		if f.RawContentDisposition != "" {
			h.Set("Content-Disposition", f.RawContentDisposition)
		} else {
			h.Set("Content-Disposition", formatDisposition(f.Name, f.Filename))
		}
		if f.MediaType != "" {
			h.Set("Content-Type", f.MediaType)
		}
		for k, v := range f.ExtraHeaders {
			h.Set(k, v)
		}
		pw, err := mw.CreatePart(h)
		if err != nil {
			panic(err)
		}
		writeDeterministic(pw, f.Size, f.Content)
	}
	if b.Truncate {
		// Keep headers/parts intact, drop the closing delimiter.
		fmt.Fprint(buf, "\r\n")
	} else {
		if err := mw.Close(); err != nil {
			panic(err)
		}
	}
	return buf.Bytes()
}

func writeDeterministic(w io.Writer, size int, content func(int) byte) {
	buf := make([]byte, 4096)
	for off := 0; off < size; off += len(buf) {
		end := min(off+len(buf), size)
		for i := off; i < end; i++ {
			fn := content
			if fn == nil {
				fn = deterministic
			}
			buf[i-off] = fn(i)
		}
		if _, err := w.Write(buf[:end-off]); err != nil {
			panic(err)
		}
	}
}

func formatDisposition(name, filename string) string {
	if filename != "" {
		return fmt.Sprintf(`form-data; name=%q; filename=%q`, name, filename)
	}
	return fmt.Sprintf(`form-data; name=%q`, name)
}

// script chunks the rendered body for the streaming source.
func (b Body) script(chunkSize int) [][]byte {
	data := b.Build()
	if chunkSize <= 0 {
		chunkSize = 32 * 1024
	}
	chunks := make([][]byte, 0, len(data)/chunkSize+1)
	for off := 0; off < len(data); off += chunkSize {
		end := min(off+chunkSize, len(data))
		chunks = append(chunks, append([]byte(nil), data[off:end]...))
	}
	return chunks
}

// fileByName returns the body file for a hook part name (last definition
// wins, matching multi-file fields the consumer observes in sequence).
func (b Body) fileByName(name string) (File, bool) {
	var f File
	ok := false
	for _, f = range b.Files {
		if f.Name == name {
			ok = true
		}
	}
	return f, ok
}
