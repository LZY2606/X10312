// Package conformance provides a single, adapter-agnostic conformance harness
// for formstream.
//
// The harness drives parsers only through their public entry points
// (formstream.Parser and the http/echo/gin adapters): it constructs real
// multipart/form-data bodies, streams them to the parser from a producer
// goroutine, and asserts observable semantics (fields, filenames, files,
// errors) together with the invariants that distinguish a streaming parser
// from a buffering one (chunked delivery, streaming gate, bounded reads,
// producer goroutine exit and temporary resource cleanup).
//
// It is internal on purpose: it exercises the production implementation
// instead of reimplementing it, and it only exists to serve the formstream
// cross-adapter conformance and leak regression tests.
package conformance

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"strings"
	"time"
)

const chunkSize = 4 * 1024

var (
	// ErrInjectedReaderFailure is injected into the multipart stream midway
	// through a file part.
	ErrInjectedReaderFailure = errors.New("conformance: injected reader failure")
	// ErrConsumerPrefixOnly is returned by a consumer which deliberately reads
	// only a prefix of a file part.
	ErrConsumerPrefixOnly = errors.New("conformance: consumer read only a prefix")
)

// PartSpec describes a single multipart part.
type PartSpec struct {
	Name      string
	FileName  string
	MediaType string
	Content   []byte
	// ExtraHeaders adds raw header lines (key -> value) after the default
	// Content-Disposition/Content-Type headers.
	ExtraHeaders map[string]string
}

// FilePart returns a file part whose deterministic content is size bytes.
func FilePart(name, fileName, mediaType string, size int) PartSpec {
	return PartSpec{
		Name:      name,
		FileName:  fileName,
		MediaType: mediaType,
		Content:   deterministicBytes(size),
	}
}

// FieldPart returns a regular form field part.
func FieldPart(name, value string) PartSpec {
	return PartSpec{Name: name, Content: []byte(value)}
}

func partHeader(part PartSpec) textproto.MIMEHeader {
	h := make(textproto.MIMEHeader)

	disposition := fmt.Sprintf(`form-data; name=%q`, part.Name)
	if part.FileName != "" {
		disposition = fmt.Sprintf(`form-data; name=%q; filename=%q`, part.Name, part.FileName)
	}
	h.Set("Content-Disposition", disposition)

	if part.MediaType != "" {
		h.Set("Content-Type", part.MediaType)
	}
	for key, value := range part.ExtraHeaders {
		h.Set(key, value)
	}

	return h
}

// Stats are the resource observations collected while a form was delivered.
type Stats struct {
	// ReadCalls counts Read calls on the body handed to the parser.
	ReadCalls int
	// MaxRead is the largest single Read size observed.
	MaxRead int
	// TotalRead is the number of bytes delivered.
	TotalRead int
	// CloseCalls counts Close calls on the body handed to the parser.
	CloseCalls int
}

type meteredReadCloser struct {
	r     io.Reader
	stats *Stats
}

func (m *meteredReadCloser) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	m.stats.ReadCalls++
	if n > m.stats.MaxRead {
		m.stats.MaxRead = n
	}
	m.stats.TotalRead += n
	return n, err
}

func (m *meteredReadCloser) Close() error {
	m.stats.CloseCalls++
	if c, ok := m.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// StreamOptions control streamed form delivery.
type StreamOptions struct {
	// FailAfter injects ErrInjectedReaderFailure after FailAfter bytes of the
	// first file part (0 disables injection).
	FailAfter int
	// Truncate drops the final boundary when true, producing a truncated form.
	Truncate bool
	// Gate enables the streaming gate on the last file part: the producer
	// writes one chunk, blocks until Release is called, and only then writes
	// the remainder and the closing boundary.
	Gate bool
}

// Source is the multipart body handed to a parser plus its observations.
type Source struct {
	io.ReadCloser
	Stats *Stats

	producerDone <-chan struct{}
	// dataFlowing is closed by the producer once the first chunk of the gated
	// file has been accepted by the pipe (i.e. the parser consumed it).
	dataFlowing chan struct{}
	release     chan struct{}
}

// NewStream builds a multipart form in a producer goroutine and streams it to
// the parser through an unbuffered pipe. The whole form is never buffered: the
// producer blocks until the parser reads each chunk, so a parser that buffers
// the whole body before invoking hooks deadlocks the gate instead of passing.
func NewStream(parts []PartSpec, opts StreamOptions) *Source {
	pr, pw := io.Pipe()
	stats := &Stats{}
	producerDone := make(chan struct{})

	var dataFlowing chan struct{}
	release := make(chan struct{})
	if opts.Gate {
		dataFlowing = make(chan struct{})
	}

	go conformanceFormProducer(producerDone, pw, parts, opts, dataFlowing, release)

	return &Source{
		ReadCloser:   &meteredReadCloser{r: pr, stats: stats},
		Stats:        stats,
		producerDone: producerDone,
		dataFlowing:  dataFlowing,
		release:      release,
	}
}

// ProducerExited reports whether the form producer goroutine has exited.
func (s *Source) ProducerExited() bool {
	select {
	case <-s.producerDone:
		return true
	default:
		return false
	}
}

// WaitProducer waits for the form producer goroutine to exit.
func WaitProducer(src *Source, timeout time.Duration) error {
	select {
	case <-src.producerDone:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("conformance: form producer goroutine did not exit within %s", timeout)
	}
}

// AwaitDataFlowing waits until the gated producer has delivered the first file
// chunk to the parser while still holding back the rest.
func (s *Source) AwaitDataFlowing(timeout time.Duration) error {
	if s.dataFlowing == nil {
		return nil
	}
	select {
	case <-s.dataFlowing:
		return nil
	case <-s.producerDone:
		return errors.New("conformance: producer exited before data was observed")
	case <-time.After(timeout):
		return errors.New("conformance: parser did not start consuming the gated stream")
	}
}

// Release unblocks a gated producer. It must be called after the consumer hook
// observed file data.
func (s *Source) Release() {
	if s.dataFlowing != nil {
		select {
		case <-s.dataFlowing:
		default:
			<-s.dataFlowing
		}
		select {
		case <-s.release:
		default:
			close(s.release)
		}
	}
}

// conformanceFormProducer is named explicitly so goroutine stacks identify the
// harness producer.
func conformanceFormProducer(done chan<- struct{}, pw *io.PipeWriter, parts []PartSpec, opts StreamOptions, dataFlowing chan<- struct{}, release <-chan struct{}) {
	defer close(done)

	err := writeStream(pw, parts, opts, dataFlowing, release)
	if err == nil {
		pw.Close()
		return
	}
	pw.CloseWithError(err)
}

func writeStream(pw io.Writer, parts []PartSpec, opts StreamOptions, dataFlowing chan<- struct{}, release <-chan struct{}) error {
	mw := multipart.NewWriter(pw)
	gatedFile := -1
	if opts.Gate {
		gatedFile = lastFileIndex(parts)
	}

	for i, part := range parts {
		partWriter, err := mw.CreatePart(partHeader(part))
		if err != nil {
			return err
		}

		switch {
		case opts.FailAfter > 0 && part.FileName != "":
			n := min(opts.FailAfter, len(part.Content))
			if err := writeChunked(partWriter, part.Content[:n]); err != nil {
				return err
			}
			return ErrInjectedReaderFailure
		case i == gatedFile:
			if err := writeGated(partWriter, part.Content, dataFlowing, release); err != nil {
				return err
			}
		default:
			if err := writeChunked(partWriter, part.Content); err != nil {
				return err
			}
		}
	}

	if opts.Truncate {
		return nil
	}

	return mw.Close()
}

func lastFileIndex(parts []PartSpec) int {
	idx := -1
	for i, part := range parts {
		if part.FileName != "" {
			idx = i
		}
	}
	return idx
}

// writeGated writes one chunk, signals that data is flowing, waits for the
// consumer to release the gate, then writes the remainder.
func writeGated(w io.Writer, content []byte, dataFlowing chan<- struct{}, release <-chan struct{}) error {
	n := min(chunkSize, len(content))
	if _, err := w.Write(content[:n]); err != nil {
		return err
	}
	close(dataFlowing)
	<-release
	return writeChunked(w, content[n:])
}

func writeChunked(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n := min(chunkSize, len(p))
		if _, err := w.Write(p[:n]); err != nil {
			return err
		}
		p = p[n:]
	}
	return nil
}

// NewMemory builds the same multipart form fully in memory. It is the control
// source used to prove results are independent of delivery and scheduling.
func NewMemory(parts []PartSpec) *Source {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, part := range parts {
		pw, err := mw.CreatePart(partHeader(part))
		if err == nil {
			_, err = pw.Write(part.Content)
		}
		if err != nil {
			panic(err)
		}
	}
	if err := mw.Close(); err != nil {
		panic(err)
	}

	data := buf.Bytes()
	stats := &Stats{TotalRead: len(data)}
	return &Source{
		ReadCloser:   &meteredReadCloser{r: bytes.NewReader(data), stats: stats},
		Stats:        stats,
		producerDone: closedChan(),
	}
}

func closedChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// StatsSnapshot returns a copy of the stats attached to the source.
func (s *Source) StatsSnapshot() Stats {
	return *s.Stats
}

// deterministicBytes produces reproducible content of the requested size
// without holding any large shared state.
func deterministicBytes(size int) []byte {
	const pattern = "formstream-conformance-0123456789-"
	out := make([]byte, size)
	for i := range out {
		out[i] = pattern[i%len(pattern)]
	}
	return out
}

// Drain reads r fully in small chunks.
func Drain(r io.Reader) (int64, error) {
	return io.Copy(io.Discard, &smallReader{r: r})
}

type smallReader struct {
	r io.Reader
}

func (s *smallReader) Read(p []byte) (int, error) {
	n := min(len(p), 256)
	return s.r.Read(p[:n])
}

// Summary renders stats for failure messages.
func (st Stats) Summary() string {
	return fmt.Sprintf("readCalls=%d maxRead=%d totalRead=%d closeCalls=%d", st.ReadCalls, st.MaxRead, st.TotalRead, st.CloseCalls)
}

// DescribeParts renders a short description of parts for failure messages.
func DescribeParts(parts []PartSpec) string {
	descriptions := make([]string, 0, len(parts))
	for _, part := range parts {
		descriptions = append(descriptions, fmt.Sprintf("{name:%q file:%q bytes:%d}", part.Name, part.FileName, len(part.Content)))
	}
	return strings.Join(descriptions, ", ")
}
