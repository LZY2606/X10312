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

	// errProducerStopped is used to unblock the producer goroutine after the
	// consumer stopped reading the multipart stream.
	errProducerStopped = errors.New("formstream: consumer stopped reading the multipart stream")
)

// Parse parses the multipart form from r.
func (p *Parser) Parse(r io.Reader) error {
	return p.ParseWithContext(context.Background(), r)
}

// ParseWithContext parses the multipart form from r, like Parse, but honors
// ctx.
//
// The form is read by a dedicated producer goroutine and handed to the parser
// through an unbuffered pipe. When ctx is canceled, the pipe is closed with
// the context cause, the blocked parser returns that cause as the first error,
// and ParseWithContext does not return until the producer goroutine has
// exited. The same guarantee holds when a hook returns an error or r fails
// midway: the first observed causal error is returned and no producer goroutine
// is leaked.
//
// A reader whose Read stays blocked forever after ctx is canceled must be
// unblocked by its owner (for example an *http.Request.Body, which is bound to
// the request context); ParseWithContext itself never closes r.
func (p *Parser) ParseWithContext(ctx context.Context, r io.Reader) (err error) {
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

	parseReader := r
	if ctx.Done() != nil {
		pr, pw := io.Pipe()

		producerDone := make(chan struct{})
		go func() {
			defer close(producerDone)
			pipeReaderToWriter(ctx, pw, r)
		}()
		parseReader = pr

		defer func() {
			// Unblock the producer: on cancellation the context cause has
			// priority, otherwise the parser has stopped consuming the stream.
			if cause := context.Cause(ctx); cause != nil {
				pr.CloseWithError(cause)
			} else {
				pr.CloseWithError(errProducerStopped)
			}
			<-producerDone
		}()
	}

	err = p.parse(parseReader, hsc.IConditionJudger)

	return
}

// pipeReaderToWriter copies r to pw without ever buffering the whole stream.
// It returns once r reaches EOF, fails, ctx is canceled, or the read side of
// the pipe is closed because parsing stopped.
func pipeReaderToWriter(ctx context.Context, pw *io.PipeWriter, r io.Reader) {
	stop := context.AfterFunc(ctx, func() {
		pw.CloseWithError(context.Cause(ctx))
	})
	defer stop()

	buf := make([]byte, 32*1024)
	for {
		if cause := context.Cause(ctx); cause != nil {
			pw.CloseWithError(cause)
			return
		}

		n, readErr := r.Read(buf)
		if n > 0 {
			if _, writeErr := pw.Write(buf[:n]); writeErr != nil {
				return
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				readErr = nil
			}
			pw.CloseWithError(readErr)
			return
		}
	}
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

	// The buffer is returned to the pool on every error and on the disk path.
	// The memory path transfers ownership to the customReadCloser.
	returnBuf := true
	defer func() {
		if returnBuf {
			bufPool.Put(buf)
		}
	}()

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
		returnBuf = false
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
