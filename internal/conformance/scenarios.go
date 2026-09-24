package conformance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mazrean/formstream"
)

// Scenario is one fixed conformance input and its expected outcome.
type Scenario struct {
	// Name identifies the contract the scenario exercises.
	Name string
	// Description carries the minimal context reported on first violation.
	Description string

	// Fields are plain non-hook form fields, in wire order.
	Fields []FieldSpec
	// Files are file parts, in wire order.
	Files []FileSpec
	// HookFields marks fields that must be registered as hooks.
	HookFields map[string]struct{}
	// RequiredPart declares each hook field's required plain fields.
	RequiredPart map[string][]string

	// Options configure the parser through public options only.
	Options []formstream.ParserOption

	// Stream produces the body through a chunked io.Pipe instead of buffering
	// it, and is required for large-file streaming scenarios.
	Stream *StreamSpec
	// LongHeader builds one part with an oversized header block.
	LongHeader *LongHeaderSpec
	// Corrupt replaces the second boundary delimiter with garbage so parsing
	// fails mid-stream with a damaged-input error.
	Corrupt bool

	// Want is the expected caller-visible result.
	Want Want
}

// FieldSpec is one plain text field.
type FieldSpec struct {
	Name    string
	Content string
}

// LongHeaderSpec forces one part's header block to exceed multipart's hard
// 10MiB header limit.
type LongHeaderSpec struct {
	Field string
	Bytes int
}

// StreamSpec drives a body over io.Pipe in fixed-size chunks.
type StreamSpec struct {
	// FileIndex selects the file whose body is streamed (others are buffered).
	FileIndex int
	ChunkSize int
	// ChunkDelay pauses the producer after every chunk (slow network model).
	ChunkDelay time.Duration
	// FailAfterBytes injects a recoverable producer failure mid-file. The
	// scenario must be retried with a fresh body; a single Parse must surface
	// this exact error as the first causal error.
	FailAfterBytes int64
	FailError      error
}

// BuildBody returns the body, live stats and a cleanup/cancel function.
func (s *Scenario) BuildBody() (io.Reader, *LiveStats, func()) {
	stats := &LiveStats{}

	if s.Stream != nil {
		return s.buildStream(stats)
	}

	return s.buildBuffered(stats)
}

func (s *Scenario) buildBuffered(stats *LiveStats) (io.Reader, *LiveStats, func()) {
	buf := bytes.NewBuffer(nil)
	mw := multipart.NewWriter(buf)
	_ = mw.SetBoundary(Boundary)

	writeAll(mw, s.Fields, s.Files)
	if s.LongHeader != nil {
		writeLongHeader(mw, s.LongHeader)
	}
	_ = mw.Close()

	body := buf.Bytes()
	if s.Corrupt {
		body = corruptSecondBoundary(body)
	}

	cr := &countReader{r: bytes.NewReader(body), stats: stats}
	return cr, stats, func() {}
}

func writeAll(mw *multipart.Writer, fields []FieldSpec, files []FileSpec) {
	for _, f := range fields {
		if err := mw.WriteField(f.Name, f.Content); err != nil {
			panic(err)
		}
	}
	for _, f := range files {
		mh := textproto.MIMEHeader{}
		if f.Filename != "" {
			mh.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, f.Name, f.Filename))
			mh.Set("Content-Type", "application/octet-stream")
		} else {
			mh.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q`, f.Name))
		}
		pw, err := mw.CreatePart(mh)
		if err != nil {
			panic(err)
		}
		if _, err := pw.Write(f.Content); err != nil {
			panic(err)
		}
	}
}

func writeLongHeader(mw *multipart.Writer, spec *LongHeaderSpec) {
	mh := textproto.MIMEHeader{}
	mh.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q`, spec.Field))
	mh.Set("X-Pad", strings.Repeat("x", spec.Bytes))
	pw, err := mw.CreatePart(mh)
	if err != nil {
		panic(err)
	}
	_, _ = pw.Write([]byte("v"))
}

func corruptSecondBoundary(body []byte) []byte {
	needle := []byte("\r\n--" + Boundary + "\r\n")
	first := bytes.Index(body, needle)
	if first < 0 {
		return body
	}
	second := bytes.Index(body[first+len(needle):], needle)
	if second < 0 {
		return body
	}
	start := first + len(needle) + second
	copy(body[start:start+len(needle)], []byte("\r\n---GARBAGE-DAMAGED-INPUT--\r\n")[:len(needle)])
	return body
}

// LiveStats are counters observable through the body reader wrapper.
type LiveStats struct {
	readCalls  atomic.Int64
	readBytes  atomic.Int64
	closeCalls atomic.Int64
	peak       atomic.Int64
}

func (l *LiveStats) snapshot() Stats {
	return Stats{
		ReadCalls:    l.readCalls.Load(),
		ReadBytes:    l.readBytes.Load(),
		CloseCalls:   l.closeCalls.Load(),
		PeakInflight: l.peak.Load(),
	}
}

type countReader struct {
	r     io.Reader
	stats *LiveStats
}

func (c *countReader) Read(p []byte) (int, error) {
	c.stats.readCalls.Add(1)
	n, err := c.r.Read(p)
	c.stats.readBytes.Add(int64(n))
	return n, err
}

func (c *countReader) Close() error {
	c.stats.closeCalls.Add(1)
	if rc, ok := c.r.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

func (s *Scenario) buildStream(stats *LiveStats) (io.Reader, *LiveStats, func()) {
	pr, pw := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := s.writeStream(ctx, pw, stats); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()

	cr := &countReader{r: pr, stats: stats}
	return cr, stats, cancel
}

func (s *Scenario) writeStream(ctx context.Context, pw *io.PipeWriter, stats *LiveStats) error {
	sp := s.Stream
	var inflight atomic.Int64
	updatePeak := func(v int64) {
		for {
			peak := stats.peak.Load()
			if v <= peak || stats.peak.CompareAndSwap(peak, v) {
				return
			}
		}
	}

	mw := multipart.NewWriter(pw)
	_ = mw.SetBoundary(Boundary)
	for _, f := range s.Fields {
		if err := mw.WriteField(f.Name, f.Content); err != nil {
			return err
		}
	}

	for i, f := range s.Files {
		mh := textproto.MIMEHeader{}
		mh.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, f.Name, f.Filename))
		mh.Set("Content-Type", "application/octet-stream")
		p, err := mw.CreatePart(mh)
		if err != nil {
			return err
		}

		if i != sp.FileIndex {
			if _, err := p.Write(f.Content); err != nil {
				return err
			}
			continue
		}

		// Chunked, demand-driven writes: io.Pipe only accepts while the parser
		// consumes, so PeakInflight stays bounded by chunk size regardless of
		// total file size.
		var sent int64
		content := f.Content
		for len(content) > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			n := sp.ChunkSize
			if n <= 0 || n > len(content) {
				n = len(content)
			}
			inflight.Add(int64(n))
			updatePeak(inflight.Load())
			if sp.FailAfterBytes > 0 && sent+int64(n) >= sp.FailAfterBytes {
				if sp.FailError != nil {
					return sp.FailError
				}
			}
			if _, err := p.Write(content[:n]); err != nil {
				// Parser stopped consuming (reader failure / early return /
				// cancellation): surface it so the producer goroutine exits.
				return fmt.Errorf("stream producer: %w", err)
			}
			inflight.Add(-int64(n))
			content = content[n:]
			sent += int64(n)
			if sp.ChunkDelay > 0 {
				time.Sleep(sp.ChunkDelay)
			}
		}
	}

	if err := mw.Close(); err != nil {
		return err
	}
	return nil
}

// ExpectedFile builds the FileRecord a correct implementation must deliver.
func ExpectedFile(f FileSpec) FileRecord {
	sum := sha256.Sum256(f.Content)
	return FileRecord{
		Field:       f.Name,
		Filename:    f.Filename,
		ContentType: "application/octet-stream",
		Size:        len(f.Content),
		SHA256:      hex.EncodeToString(sum[:]),
	}
}

// CancellingReader wraps a body and returns ctx.Err once cancelled, modeling a
// request body whose transport context is canceled mid-read.
type CancellingReader struct {
	r      io.Reader
	ctx    context.Context
	stats  *LiveStats
	closed atomic.Int64
}

func NewCancellingReader(ctx context.Context, r io.Reader, stats *LiveStats) *CancellingReader {
	return &CancellingReader{r: r, ctx: ctx, stats: stats}
}

func (c *CancellingReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		c.stats.readCalls.Add(1)
		return 0, err
	}
	c.stats.readCalls.Add(1)
	n, err := c.r.Read(p)
	c.stats.readBytes.Add(int64(n))
	return n, err
}

func (c *CancellingReader) Close() error {
	if c.closed.Add(1) != 1 {
		return nil
	}
	c.stats.closeCalls.Add(1)
	if rc, ok := c.r.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

// Ensure imports used only in helpers stay referenced.
var (
	_ = errors.Is
	_ = sync.Mutex{}
)
