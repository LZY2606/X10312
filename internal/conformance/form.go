package conformance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"os"
)

// PartSpec describes a single multipart part in a form.
type PartSpec struct {
	Field       string
	FileName    string
	ContentType string
	// Size is the number of deterministic pattern bytes for a file part.
	Size int64
	// Value is the exact field value for a plain field part.
	Value string
	// IsField marks a plain form field.
	IsField bool
	// ExtraHeader adds header lines beyond Content-Disposition/Content-Type.
	ExtraHeader map[string]string
}

// FormSpec describes a deterministic multipart form.
type FormSpec struct {
	Parts []PartSpec
	// RawForm, when non-empty, is written verbatim as the entire body and
	// bypasses mime/multipart.Writer. It is used for crafted long-header
	// and malformed-input bodies.
	RawForm string
}

// Boundary returns the harness multipart boundary.
func Boundary() string { return boundary }

// BuildFile writes the form to a new temporary file and returns it with its
// size. The caller owns the file and must close it. The form itself is
// never held in memory by the harness.
func BuildFile(spec FormSpec) (*os.File, int64, error) {
	f, err := os.CreateTemp("", "formstream-conformance-form-")
	if err != nil {
		return nil, 0, fmt.Errorf("create temp form: %w", err)
	}

	if spec.RawForm != "" {
		if _, err := io.WriteString(f, spec.RawForm); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return nil, 0, fmt.Errorf("write raw form: %w", err)
		}
	} else {
		if err := writeForm(f, spec); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return nil, 0, err
		}
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, 0, fmt.Errorf("stat form: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, 0, fmt.Errorf("rewind form: %w", err)
	}

	return f, fi.Size(), nil
}

func writeForm(w io.Writer, spec FormSpec) error {
	mw := multipart.NewWriter(w)
	if err := mw.SetBoundary(boundary); err != nil {
		return fmt.Errorf("set boundary: %w", err)
	}

	for _, part := range spec.Parts {
		if part.IsField {
			if err := mw.WriteField(part.Field, part.Value); err != nil {
				return fmt.Errorf("write field %q: %w", part.Field, err)
			}
			continue
		}

		mh := make(textproto.MIMEHeader)
		disposition := fmt.Sprintf(`form-data; name=%q`, part.Field)
		if part.FileName != "" {
			disposition += fmt.Sprintf(`; filename=%q`, part.FileName)
		}
		mh.Set("Content-Disposition", disposition)
		if part.ContentType != "" {
			mh.Set("Content-Type", part.ContentType)
		}
		for k, v := range part.ExtraHeader {
			mh.Set(k, v)
		}

		pw, err := mw.CreatePart(mh)
		if err != nil {
			return fmt.Errorf("create part %q: %w", part.Field, err)
		}
		if _, err := io.CopyN(pw, patternReader(part.Field), part.Size); err != nil {
			return fmt.Errorf("write body %q: %w", part.Field, err)
		}
	}

	if err := mw.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	return nil
}

// patternReader returns an infinite deterministic byte stream keyed by the
// field name, so checksums stay reproducible across adapters and attempts.
func patternReader(key string) io.Reader {
	seed := sha256.Sum256([]byte("formstream-pattern:" + key))
	return &seqReader{seed: seed[:]}
}

type seqReader struct {
	seed []byte
	off  int64
}

func (r *seqReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.seed[(r.off+int64(i))%int64(len(r.seed))]
	}
	r.off += int64(len(p))
	return len(p), nil
}

// PatternChecksum returns the expected SHA-256 of the first n pattern bytes
// for key.
func PatternChecksum(key string, n int64) string {
	h := sha256.New()
	_, _ = io.CopyN(h, patternReader(key), n)
	return hex.EncodeToString(h.Sum(nil))
}
