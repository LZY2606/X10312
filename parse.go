package formstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"

	"github.com/mazrean/formstream/internal/bufpool"
	conditionjudge "github.com/mazrean/formstream/internal/condition_judge"
	"github.com/mazrean/formstream/internal/pump"
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

// ParseContext parses the multipart form from r.
//
// The body is pumped through a bounded producer goroutine so that slow
// consumers never buffer the whole request. When ctx is canceled, the
// producer is stopped (closing r as well when it implements io.Closer) and
// the first causal error is preserved.
func (p *Parser) ParseContext(ctx context.Context, r io.Reader) (err error) {
	hsc := newHookSatisfactionChecker(p.hookMap, &p.parserConfig)
	pm := pump.New(ctx, r)

	defer func() {
		stopCause := err
		if stopCause == nil {
			// Even on the successful path the source must be closed when it is
			// a Closer: the producer may have a trailing blocked Read (for
			// example request bodies behind a server). Closing unblocks it and
			// guarantees no goroutine escapes.
			stopCause = io.EOF
		}
		pm.Stop(stopCause)
	}()

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

	err = p.parse(pm.Reader(), hsc)

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
			err = p.consumeValue(part, header)
			if err != nil {
				return err
			}
		}

		err = hsc.KeyEvent(part.FormName())
		if err != nil {
			return fmt.Errorf("failed to run satisfied hook: %w", err)
		}
	}

	return nil
}

func (p *Parser) consumeValue(part *multipart.Part, header Header) (err error) {
	b := bufpool.Default.Get()
	defer bufpool.Default.Put(b)

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
		content: bytes.Clone(b.Bytes()),
		header:  header,
	})

	return nil
}

type hookSatisfactionChecker struct {
	*conditionjudge.ConditionJudger[string, *normalParam, *abnormalParam]
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

	judger := conditionjudge.NewConditionJudger(
		judgeHooks,
		preProcess.run,
		conditionjudge.WithCloseFunc[string, *normalParam, *abnormalParam](func(param *abnormalParam) error {
			return param.content.Close()
		}),
	)

	return &hookSatisfactionChecker{
		ConditionJudger: judger,
		preProcessor:    preProcess,
	}
}

func (wh *hookSatisfactionChecker) Close() error {
	return errors.Join(
		wh.ConditionJudger.Close(),
		wh.preProcessor.Close(),
	)
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

func (pp *preProcessor) run(normalParam *normalParam) (*abnormalParam, error) {
	buf := bufpool.Default.Get()

	memLimit := min(pp.config.maxMemFileSize, pp.config.maxMemSize)
	n, err := io.CopyN(buf, normalParam.r, int64(memLimit)+1)
	if err != nil && !errors.Is(err, io.EOF) {
		bufpool.Default.Put(buf)
		return nil, fmt.Errorf("failed to copy: %w", err)
	}

	var content io.ReadCloser
	if DataSize(n) > memLimit {
		if pp.file == nil {
			f, createErr := os.CreateTemp("", "formstream-")
			if createErr != nil {
				bufpool.Default.Put(buf)
				return nil, fmt.Errorf("failed to create temp file: %w", createErr)
			}
			pp.file = f
		}

		bufSize, err := io.Copy(pp.file, buf)
		if err != nil {
			bufpool.Default.Put(buf)
			return nil, fmt.Errorf("failed to write: %w", err)
		}
		bufpool.Default.Put(buf)

		remainSize, err := io.Copy(pp.file, normalParam.r)
		if err != nil {
			return nil, fmt.Errorf("failed to copy: %w", err)
		}

		size := bufSize + remainSize
		content = io.NopCloser(io.NewSectionReader(pp.file, pp.offset, size))
		pp.offset += size
	} else {
		pp.config.maxMemSize -= DataSize(buf.Len())
		pp.config.maxMemFileSize -= DataSize(buf.Len())
		bufSize := buf.Len()
		data := bytes.Clone(buf.Bytes())
		bufpool.Default.Put(buf)
		content = customReadCloser{
			Reader: bytes.NewReader(data),
			closeFunc: func() error {
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
