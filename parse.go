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
	"sync/atomic"

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
func (p *Parser) Parse(r io.Reader) (err error) {
	return p.ParseContext(context.Background(), r)
}

// ParseContext behaves like [Parser.Parse], but aborts as soon as ctx is
// canceled. When parsing is aborted for any reason (hook error, malformed
// input, budget exhaustion or cancellation), r is closed if it implements
// io.Closer, so that a producer goroutine writing to r cannot outlive the
// parse call. The first causal error is always preserved: cancellation is
// reported as ctx.Err(), while an earlier hook or reader error keeps
// precedence.
func (p *Parser) ParseContext(ctx context.Context, r io.Reader) (err error) {
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

	// Background-like contexts never fire, so avoid the watcher overhead on
	// the hot path. For cancellable contexts, closing the source unblocks a
	// Read currently waiting on a slow producer; the stop channel reaps the
	// watcher when parsing finishes before cancellation.
	if ctx.Done() != nil {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				if c, ok := r.(io.Closer); ok {
					_ = c.Close()
				}
			case <-stop:
			}
		}()
	}

	// On failure, release the source so a streaming producer (io.Pipe writer,
	// HTTP request body uploader, ...) is unblocked instead of leaking the
	// goroutine. Successful parses consume the whole stream as before.
	defer func() {
		if err != nil {
			if c, ok := r.(io.Closer); ok {
				_ = c.Close()
			}
		}
	}()

	err = p.parse(newContextReader(ctx, r), hsc.IConditionJudger)

	return
}

// contextReader turns context cancellation into a read error while leaving
// all other reads byte-identical to the production source.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func newContextReader(ctx context.Context, r io.Reader) io.Reader {
	if ctx.Done() == nil {
		return r
	}
	return &contextReader{ctx: ctx, r: r}
}

func (cr *contextReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}

	n, err := cr.r.Read(p)
	// A blocked read can only notice cancellation when the watcher closes the
	// source. Surface the cancellation itself so the causal error survives.
	if cerr := cr.ctx.Err(); cerr != nil {
		return n, cerr
	}

	return n, err
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
	// pending keeps memory-spooled contents that have not been handed to a
	// hook yet. They must be released even when parsing aborts, otherwise the
	// pooled buffer and the consumed memory budget leak past a failed parse.
	pending []*pendingMemory
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
		bufPool.Put(buf)
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
			bufPool.Put(buf)
			return nil, fmt.Errorf("failed to write: %w", err)
		}

		remainSize, err := io.Copy(pp.file, normalParam.r)
		if err != nil {
			bufPool.Put(buf)
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
		mem := &pendingMemory{
			release: func() {
				bufPool.Put(buf)
				pp.config.maxMemSize += DataSize(bufSize)
				pp.config.maxMemFileSize += DataSize(bufSize)
			},
		}
		pp.pending = append(pp.pending, mem)
		content = customReadCloser{
			Reader:    buf,
			closeFunc: func() error { mem.Release(); return nil },
		}
	}

	return &abnormalParam{
		content: content,
		header:  normalParam.h,
	}, nil
}

func (pp *preProcessor) Close() error {
	// Release any memory-spooled parts which never reached a hook because
	// parsing was aborted. Successful hooks close their own content, so only
	// still-open parts are present here.
	for _, mem := range pp.pending {
		mem.Release()
	}
	pp.pending = nil

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

// pendingMemory reclaims a memory-spooled part which never reached a hook
// because parsing was aborted. Release is idempotent: the hook closes the
// content on the completion path and preProcessor.Close runs on the abort
// path, but only one of them reclaims the same part.
type pendingMemory struct {
	release func()
	closed  atomic.Bool
}

func (m *pendingMemory) Release() {
	if m.closed.CompareAndSwap(false, true) {
		m.release()
	}
}
