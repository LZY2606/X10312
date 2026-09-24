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
// It is equivalent to ParseContext with a background context.
func (p *Parser) Parse(r io.Reader) error {
	return p.ParseContext(context.Background(), r)
}

// ParseContext parses the multipart form from r.
// When ctx is canceled, parsing stops and the cancellation error is returned.
// Each call works on a fresh snapshot of the resource budgets and resets the
// parsed values, so a parser can be reused after a failed or canceled call
// without inheriting half-updated counters, cached values, or temporary state.
func (p *Parser) ParseContext(ctx context.Context, r io.Reader) (err error) {
	cfg := p.parserConfig
	p.valueMap = make(map[string][]Value)

	hsc := newHookSatisfactionChecker(p.hookMap, &cfg, ctx)
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

	err = p.parse(ctx, r, hsc.IConditionJudger, &cfg)

	return
}

func (p *Parser) parse(ctx context.Context, r io.Reader, hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam], cfg *parserConfig) error {
	mr := multipart.NewReader(r, p.boundary)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var part *multipart.Part
		part, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next part: %w", err)
		}

		if cfg.maxParts == 0 {
			return ErrTooManyParts
		}
		cfg.maxParts--

		for _, header := range part.Header {
			if cfg.maxHeaders < uint(len(header)) {
				return ErrTooManyHeaders
			}
			cfg.maxHeaders -= uint(len(header))
		}

		header := newHeader(part.Header)
		partReader := ctxReader{ctx: ctx, r: part}
		if hsc.IsHookExist(part.FormName()) {
			_, err := hsc.HookEvent(part.FormName(), &normalParam{
				r: partReader,
				h: header,
			})
			if err != nil {
				return fmt.Errorf("failed to run or set hook: %w", err)
			}
		} else {
			b := new(bytes.Buffer)

			if DataSize(len(part.FormName())) > cfg.maxMemSize {
				return ErrTooLargeForm
			}
			cfg.maxMemSize -= DataSize(len(part.FormName()))

			n, err := io.Copy(b, partReader)
			if err != nil {
				return fmt.Errorf("failed to copy part: %w", err)
			}

			if uint64(n) > uint64(cfg.maxMemSize) {
				return ErrTooLargeForm
			}
			cfg.maxMemSize -= DataSize(n)

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

// ctxReader aborts reads with the context error once ctx is canceled.
// A nil ctx never cancels.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr ctxReader) Read(p []byte) (int, error) {
	if cr.ctx != nil {
		if err := cr.ctx.Err(); err != nil {
			return 0, err
		}
	}

	return cr.r.Read(p)
}

type hookSatisfactionChecker struct {
	conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam]
	preProcessor *preProcessor
}

func newHookSatisfactionChecker(streamHooks map[string]streamHook, config *parserConfig, ctx context.Context) *hookSatisfactionChecker {
	judgeHooks := make(map[string]conditionjudge.Hook[string, *normalParam, *abnormalParam], len(streamHooks))
	for name, hook := range streamHooks {
		h := judgeHook(hook)
		judgeHooks[name] = &h
	}

	preProcess := &preProcessor{
		ctx:    ctx,
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
	ctx    context.Context
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

	r := ctxReader{ctx: pp.ctx, r: normalParam.r}

	memLimit := min(pp.config.maxMemFileSize, pp.config.maxMemSize)
	n, err := io.CopyN(buf, r, int64(memLimit)+1)
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

		remainSize, err := io.Copy(pp.file, r)
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
