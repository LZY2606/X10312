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
func (p *Parser) Parse(r io.Reader) (err error) {
	return p.ParseContext(context.Background(), r)
}

// ParseContext behaves like Parse but aborts when ctx is canceled.
//
// A canceled context unblocks a parse that is stalled inside a slow or
// failing consumer by closing r when it implements io.Closer, and always
// waits for the internal parse routine to return before returning.
// The causal error is preserved: an error raised by the underlying reader,
// a hook or a resource limit is returned as-is (optionally joined with the
// context error when cancellation races with it), never replaced by the
// context error.
func (p *Parser) ParseContext(ctx context.Context, r io.Reader) (err error) {
	// A per-run snapshot keeps this Parser reusable: budgets consumed by one
	// invocation (successful or failed) never leak into the next one.
	runConfig := p.parserConfig
	p.valueMap = make(map[string][]Value)

	hsc := newHookSatisfactionChecker(p.hookMap, &runConfig)
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

	done := make(chan error, 1)

	// Contexts without a cancellation channel (context.Background/TODO and
	// other never-canceling contexts) keep the original zero-goroutine path,
	// so the fast path and benchmarks are unchanged.
	if ctx.Done() == nil {
		return p.parseWithConfig(r, &runConfig, hsc.IConditionJudger)
	}

	go func() {
		done <- p.parseWithConfig(r, &runConfig, hsc.IConditionJudger)
	}()

	select {
	case parseErr := <-done:
		return parseErr
	case <-ctx.Done():
		// Unblock the parse routine if it is waiting on the input reader.
		if c, ok := r.(io.Closer); ok {
			_ = c.Close()
		}
		parseErr := <-done
		if parseErr == nil {
			return ctx.Err()
		}
		return errors.Join(parseErr, ctx.Err())
	}

}

func (p *Parser) parse(r io.Reader, hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]) error {
	return p.parseWithConfig(r, &p.parserConfig, hsc)
}

func (p *Parser) parseWithConfig(r io.Reader, config *parserConfig, hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]) error {
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

		if config.maxParts == 0 {
			return ErrTooManyParts
		}
		config.maxParts--

		for _, header := range part.Header {
			if config.maxHeaders < uint(len(header)) {
				return ErrTooManyHeaders
			}
			config.maxHeaders -= uint(len(header))
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

			if DataSize(len(part.FormName())) > config.maxMemSize {
				return ErrTooLargeForm
			}
			config.maxMemSize -= DataSize(len(part.FormName()))

			n, err := io.Copy(b, part)
			if err != nil {
				return fmt.Errorf("failed to copy part: %w", err)
			}

			if uint64(n) > uint64(config.maxMemSize) {
				return ErrTooLargeForm
			}
			config.maxMemSize -= DataSize(n)

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
	config        *parserConfig
	offset        int64
	file          *os.File
	pendingMemory []*customReadCloser
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
				bufPool.Put(buf)
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
		bufPool.Put(buf)

		size := bufSize + remainSize
		content = io.NopCloser(io.NewSectionReader(pp.file, pp.offset, size))
		pp.offset += size
	} else {
		pp.config.maxMemSize -= DataSize(buf.Len())
		pp.config.maxMemFileSize -= DataSize(buf.Len())
		bufSize := buf.Len()
		content = &customReadCloser{
			Reader: buf,
		}
		closer := content.(*customReadCloser)
		closer.closeFunc = func() error {
			bufPool.Put(buf)
			pp.config.maxMemSize += DataSize(bufSize)
			pp.config.maxMemFileSize += DataSize(bufSize)
			return nil
		}
		pp.pendingMemory = append(pp.pendingMemory, closer)
	}

	return &abnormalParam{
		content: content,
		header:  normalParam.h,
	}, nil
}

func (pp *preProcessor) Close() error {
	// Parts that were spooled into memory but whose hook never ran (or never
	// closed the reader because parsing aborted) still own pooled buffers and
	// memory budget. Release them so a failed parse leaves neither behind.
	var pendingErr error
	for _, closer := range pp.pendingMemory {
		if err := closer.Close(); err != nil {
			pendingErr = errors.Join(pendingErr, err)
		}
	}
	pp.pendingMemory = nil

	if pp.file == nil {
		return pendingErr
	}

	filepath := pp.file.Name()

	// Close the file handle first
	closeErr := pp.file.Close()

	// Remove the temporary file from disk
	removeErr := os.Remove(filepath)

	// Return combined errors if any
	if closeErr != nil || removeErr != nil || pendingErr != nil {
		return errors.Join(pendingErr, closeErr, removeErr)
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
	closeOnce sync.Once
	closeFunc func() error
}

func (cc *customReadCloser) Close() error {
	var err error
	cc.closeOnce.Do(func() {
		err = cc.closeFunc()
	})
	return err
}
