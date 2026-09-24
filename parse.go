package formstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"sync"

	conditionjudge "github.com/mazrean/formstream/internal/condition_judge"
)

var (
	// ErrTooManyParts is returned when the parts are more than MaxParts.
	ErrTooManyParts = errors.New("too many parts")
	// ErrTooManyHeaders is returned when the headers are more than MaxHeaders.
	ErrTooManyHeaders = errors.New("too many headers")
	// ErrTooLargeForm is returned when the form is too large for the parser to handle within the memory limit.
	ErrTooLargeForm = errors.New("too large form")
)

// Parse parses the multipart form from r.
// It is equivalent to [Parser.ParseWithContext] with [context.Background].
func (p *Parser) Parse(r io.Reader) error {
	return p.ParseWithContext(context.Background(), r)
}

// ParseWithContext parses the multipart form from r.
//
// The context only governs cancellation: when ctx is canceled, in-flight
// reads are unblocked and the context's error is preserved as the first
// cause. When parsing terminates because of any error (including a hook
// error, a reader error or cancellation), r is closed when it implements
// [io.Closer]. On success r is left open, following the net/http convention
// where the caller owns the request body.
func (p *Parser) ParseWithContext(ctx context.Context, r io.Reader) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	hsc := newHookSatisfactionChecker(p.hookMap, &p.parserConfig)
	defer func() {
		deferErr := hsc.Close()
		// capture the error of Close()
		if deferErr != nil {
			if err != nil {
				err = errors.Join(err, deferErr)
			} else {
				err = deferErr
			}
		}
	}()

	source := r
	var cancelable *cancelableReader
	if ctx.Done() != nil {
		cancelable = newCancelableReader(ctx, r)
		defer func() {
			cancelable.release(err == nil)
		}()
		source = cancelable
	}

	err = p.parse(source, hsc.IConditionJudger)

	return
}

func (p *Parser) parse(r io.Reader, hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]) error {
	mr := multipart.NewReader(r, p.boundary)
	for {
		var part *multipart.Part
		part, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next part: %w", err)
		}

		if p.maxParts == 0 {
			return ErrTooManyParts
		}
		p.maxParts--

		for _, header := range part.Header {
			if p.maxHeaders < uint(len(header)) {
				return ErrTooManyHeaders
			}
			p.maxHeaders -= uint(len(header))
		}

		header := newHeader(part.Header)
		if hsc.IsHookExist(part.FormName()) {
			_, err := hsc.HookEvent(part.FormName(), &normalParam{
				r: part,
				h: header,
			})
			if err != nil {
				return fmt.Errorf("failed to run or set hook: %w", err)
			}
		} else {
			b := new(bytes.Buffer)

			if DataSize(len(part.FormName())) > p.maxMemSize {
				return ErrTooLargeForm
			}
			p.maxMemSize -= DataSize(len(part.FormName()))

			n, err := io.Copy(b, part)
			if err != nil {
				return fmt.Errorf("failed to copy part: %w", err)
			}

			if uint64(n) > uint64(p.maxMemSize) {
				return ErrTooLargeForm
			}
			p.maxMemSize -= DataSize(n)

			p.valueMap[part.FormName()] = append(p.valueMap[part.FormName()], Value{
				content: b.Bytes(),
				header:  header,
			})
		}

		err = hsc.KeyEvent(part.FormName())
		if err != nil {
			return fmt.Errorf("failed to run satisfied hook: %w", err)
		}
	}

	return nil
}

type hookSatisfactionChecker struct {
	conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]
	preProcessor *preProcessor
}

func newHookSatisfactionChecker(streamHooks map[string]streamHook, config *parserConfig) *hookSatisfactionChecker {
	judgeHooks := make(map[string]conditionjudge.Hook[string, *normalParam, *abnormalParam], len(streamHooks))
	for name, hook := range streamHooks {
		h := judgeHook(hook)
		judgeHooks[name] = &h
	}

	preProcess := &preProcessor{
		config: config,
	}

	return &hookSatisfactionChecker{
		IConditionJudger: conditionjudge.NewConditionJudger(judgeHooks, preProcess.run),
		preProcessor:     preProcess,
	}
}

func (wh *hookSatisfactionChecker) Close() error {
	return wh.preProcessor.Close()
}

// cancelableReader bridges context cancellation to a synchronous reader.
//
// The parser reads from a single goroutine. When the context is canceled,
// the watcher closes the underlying reader (when it implements
// [io.Closer]); this unblocks any in-flight read performed by a producer on
// the other end (e.g. a request body being uploaded) so that goroutine can
// exit. The watcher itself exits as soon as reading finishes.
type cancelableReader struct {
	ctx    context.Context
	r      io.Reader
	closer io.Closer
	stop   chan struct{}
}

func newCancelableReader(ctx context.Context, r io.Reader) *cancelableReader {
	cr := &cancelableReader{
		ctx:  ctx,
		r:    r,
		stop: make(chan struct{}),
	}
	if closer, ok := r.(io.Closer); ok {
		cr.closer = closer
	}
	if cr.closer != nil {
		go cr.watch()
	}
	return cr
}

func (cr *cancelableReader) watch() {
	select {
	case <-cr.ctx.Done():
		// Unblock any in-flight read so the producer can exit.
		_ = cr.closer.Close()
	case <-cr.stop:
	}
}

func (cr *cancelableReader) Read(p []byte) (int, error) {
	// Fail fast if the context is already canceled.
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}

	n, err := cr.r.Read(p)
	// A cancellation wins over a concurrent read error (such as the error
	// caused by closing the reader) so the first causal error is preserved.
	if ctxErr := cr.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}

// release stops the watcher and, on failure, closes the source. Closing the
// body after an aborted parse is the net/http convention and guarantees a
// producer blocked on writing the remaining body is released. On success the
// source is left for the caller to close.
func (cr *cancelableReader) release(success bool) {
	close(cr.stop)
	if !success && cr.closer != nil {
		_ = cr.closer.Close()
	}
}

type normalParam struct {
	r io.Reader
	h Header
}

type abnormalParam struct {
	content io.ReadCloser
	header  Header
}

type preProcessor struct {
	config *parserConfig
	offset int64
	file   *os.File
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (pp *preProcessor) run(normalParam *normalParam) (*abnormalParam, error) {
	buf, ok := bufPool.Get().(*bytes.Buffer)
	if !ok {
		buf = new(bytes.Buffer)
	}
	buf.Reset()

	memLimit := min(pp.config.maxMemFileSize, pp.config.maxMemSize)
	n, err := io.CopyN(buf, normalParam.r, int64(memLimit)+1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to copy: %w", err)
	}

	var content io.ReadCloser
	if DataSize(n) > memLimit {
		if pp.file == nil {
			f, err := os.CreateTemp("", "formstream-")
			if err != nil {
				return nil, fmt.Errorf("failed to create temp file: %w", err)
			}
			pp.file = f
		}

		bufSize, err := io.Copy(pp.file, buf)
		if err != nil {
			return nil, fmt.Errorf("failed to write: %w", err)
		}

		remainSize, err := io.Copy(pp.file, normalParam.r)
		if err != nil {
			return nil, fmt.Errorf("failed to copy: %w", err)
		}

		size := bufSize + remainSize
		content = io.NopCloser(io.NewSectionReader(pp.file, pp.offset, size))
		pp.offset += size

		bufPool.Put(buf)
	} else {
		pp.config.maxMemSize -= DataSize(buf.Len())
		pp.config.maxMemFileSize -= DataSize(buf.Len())
		bufSize := buf.Len()
		content = customReadCloser{
			Reader: buf,
			closeFunc: func() error {
				bufPool.Put(buf)
				pp.config.maxMemSize += DataSize(bufSize)
				pp.config.maxMemFileSize += DataSize(bufSize)
				return nil
			},
		}
	}

	return &abnormalParam{
		content: content,
		header:  normalParam.h,
	}, nil
}

func (pp *preProcessor) Close() error {
	if pp.file == nil {
		return nil
	}

	filepath := pp.file.Name()

	// Close the file handle first
	closeErr := pp.file.Close()

	// Remove the temporary file from disk
	removeErr := os.Remove(filepath)

	// Return combined errors if any
	if closeErr != nil || removeErr != nil {
		return errors.Join(closeErr, removeErr)
	}

	return nil
}

type judgeHook struct {
	fn           StreamHookFunc
	requireParts []string
}

func (jh judgeHook) NormalPath(normalParam *normalParam) error {
	return jh.fn(normalParam.r, normalParam.h)
}

func (jh judgeHook) AbnormalPath(abnoramlParam *abnormalParam) error {
	defer abnoramlParam.content.Close()

	return jh.fn(abnoramlParam.content, abnoramlParam.header)
}

func (jh judgeHook) Requirements() []string {
	return jh.requireParts
}

type customReadCloser struct {
	io.Reader
	closeFunc func() error
}

func (cc customReadCloser) Close() error {
	return cc.closeFunc()
}
