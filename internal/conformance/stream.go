package conformance

import (
	"io"
	"mime/multipart"
)

// streamWriter renders a Body in bounded pieces. It is used for streaming
// sources so the test process itself never holds the whole large file.
type streamWriter struct {
	b          Body
	mw         *multipart.Writer
	pw         *io.PipeWriter
	closed     bool
}

// streamingBody returns a reader producing the multipart body on demand and
// a channel closed when the producer goroutine has exited. FailAfter/FailErr
// are honored by the tracked reader wrapping the result.
func (b Body) streamingBody() (io.Reader, <-chan struct{}) {
	pr, pw := io.Pipe()
	sw := &streamWriter{b: b, pw: pw}
	sw.mw = multipart.NewWriter(pw)
	_ = sw.mw.SetBoundary(Boundary)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sw.run(pr)
	}()
	return pr, done
}

func (sw *streamWriter) run(_ *io.PipeReader) {
	defer func() {
		if !sw.closed {
			_ = sw.mw.Close()
			sw.closed = true
		}
		sw.pw.Close()
	}()

	for _, f := range sw.b.Fields {
		if err := sw.mw.WriteField(f.Name, f.Value); err != nil {
			return
		}
	}

	for _, file := range sw.b.Files {
		h := sw.b.header(file)
		w, err := sw.mw.CreatePart(h)
		if err != nil {
			return
		}
		buf := make([]byte, 4096)
		for off := 0; off < file.Size; off += len(buf) {
			end := min(off+len(buf), file.Size)
			for i := off; i < end; i++ {
				fn := file.Content
				if fn == nil {
					fn = deterministic
				}
				buf[i-off] = fn(i)
			}
			if _, err := w.Write(buf[:end-off]); err != nil {
				return
			}
		}
	}

	if sw.b.Truncate {
		// Write a trailing fragment but no closing boundary.
		_, _ = sw.pw.Write([]byte("\r\n"))
		sw.closed = true
		return
	}

	sw.closed = true
	_ = sw.mw.Close()
}

// header returns the MIMEHeader for a file (mirrors Build without buffering).
func (b Body) header(f File) map[string][]string {
	h := make(map[string][]string)
	if f.RawContentDisposition != "" {
		h["Content-Disposition"] = []string{f.RawContentDisposition}
	} else {
		h["Content-Disposition"] = []string{formatDisposition(f.Name, f.Filename)}
	}
	if f.MediaType != "" {
		h["Content-Type"] = []string{f.MediaType}
	}
	for k, v := range f.ExtraHeaders {
		h[k] = []string{v}
	}
	return h
}
