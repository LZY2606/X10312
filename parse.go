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
func (p *Parser) Parse(r io.Reader) error {
	return p.ParseContext(context.Background(), r)
}

// ParseContext parses the multipart form from r.
//
// The context governs the lifetime of the parsing goroutines: if ctx is
// canceled while the parser is blocked reading from r, the parse is aborted
// and the context error is reported (joined after any earlier causal error).
// Parsing is performed incrementally without buffering the whole form: hook
// readers are fed straight from the form stream, and only non-hook fields and
// pre-processed (deferred) parts occupy memory or temporary storage.
//
// ParseContext always waits for all goroutines it started to exit before
// returning, and releases every pre-processed part that was not consumed by a
// hook, so no pooled buffer, memory budget or temporary file outlives the call.
func (p *Parser) ParseContext(ctx context.Context, r io.Reader) (err error) {
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

	// A pipe is the single handoff point of the production path: the
	// multipart reader consumes pr, while the feeder goroutine copies the
	// caller's reader into pw. Closing pw with an error makes every blocked
	// read (including a hook mid-part) fail immediately, which guarantees
	// that the parsing goroutine exits on reader failure, consumer stop or
	// context cancellation.
	pr, pw := io.Pipe()
	parseDone := make(chan struct{})

	var (
		wg        sync.WaitGroup
		abortOnce sync.Once
		abortErr  error
	)
	abort := func(cause error) {
		abortOnce.Do(func() {
			abortErr = cause
			closePipeWithCause(pw, cause)
			// Unblock a feeder that is stuck writing into the pipe, and a
			// source read whose Reader supports interruption.
			if c, ok := r.(io.Closer); ok {
				_ = c.Close()
			}
		})
	}

	// watcher unblocks the pipeline when ctx is done; it exits as soon as
	// parsing finishes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			abort(ctx.Err())
		case <-parseDone:
		}
	}()

	// feeder is the producer side of the stream: it copies r into pw
	// incrementally (fixed-size copy buffer), so the whole form is never held
	// in memory. A read error from r becomes the causal error of the stream.
	var feederErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			if ctxErr := ctx.Err(); ctxErr != nil {
				abort(ctxErr)
				return
			}

			n, readErr := r.Read(buf)
			if n > 0 {
				if _, writeErr := pw.Write(buf[:n]); writeErr != nil {
					// The parser side closed the pipe (consumer stop / parse
					// failure). Stop producing; this is not a causal error.
					return
				}
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					_ = pw.Close()
					return
				}
				feederErr = readErr
				abort(fmt.Errorf("failed to read form: %w", readErr))
				return
			}
		}
	}()

	var parseErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(parseDone)
		defer func() {
			// The parsing goroutine is the single owner of the checker; any
			// exit other than a clean EOF aborts the stream so that blocked
			// feeder/watcher goroutines unblock.
			if parseErr != nil {
				abort(parseErr)
			} else {
				_ = pw.Close()
			}
		}()
		parseErr = p.parse(pr, hsc.IConditionJudger)
	}()

	wg.Wait()

	// Prefer the first causal error. The parsing error preserves the error
	// layering (sentinel errors stay errors.Is-able); the source read error is
	// only surfaced when parsing itself did not fail, and context cancellation
	// is joined rather than replacing the cause.
	if parseErr != nil {
		err = parseErr
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = errors.Join(err, ctxErr)
		}
		return
	}
	if abortErr != nil && !errors.Is(abortErr, context.Canceled) && !errors.Is(abortErr, context.DeadlineExceeded) {
		err = abortErr
		return
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
		return
	}
	err = feederErr

	return
}

func closePipeWithCause(pw *io.PipeWriter, cause error) {
	if cause == nil {
		_ = pw.Close()
		return
	}
	_ = pw.CloseWithError(cause)
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
	preProcess := &preProcessor{
		config:         config,
		pendingContent: make(map[*abnormalParam]struct{}),
	}

	judgeHooks := make(map[string]conditionjudge.Hook[string, *normalParam, *abnormalParam], len(streamHooks))
	for name, hook := range streamHooks {
		h := judgeHook{
			fn:           hook.fn,
			requireParts: hook.requireParts,
			preProcessor: preProcess,
		}
		judgeHooks[name] = &h
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
	// pendingContent tracks every pre-processed part whose AbnormalPath has
	// not run yet, so buffers and temporary-file sections are released even
	// when parsing stops before the deferred hook becomes satisfiable.
	pendingContent map[*abnormalParam]struct{}
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

	param := &abnormalParam{
		content: content,
		header:  normalParam.h,
	}
	if pp.pendingContent == nil {
		pp.pendingContent = make(map[*abnormalParam]struct{})
	}
	pp.pendingContent[param] = struct{}{}

	return param, nil
}

// release stops tracking a pre-processed part and closes its content if its
// AbnormalPath never consumed it.
func (pp *preProcessor) release(param *abnormalParam) {
	if _, ok := pp.pendingContent[param]; !ok {
		return
	}
	delete(pp.pendingContent, param)
	_ = param.content.Close()
}

func (pp *preProcessor) Close() error {
	for param := range pp.pendingContent {
		delete(pp.pendingContent, param)
		_ = param.content.Close()
	}

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
	preProcessor *preProcessor
}

func (jh *judgeHook) NormalPath(normalParam *normalParam) error {
	return jh.fn(normalParam.r, normalParam.h)
}

func (jh *judgeHook) AbnormalPath(abnoramlParam *abnormalParam) (err error) {
	// The content must be released even when the hook returns early without
	// reading the whole part, or when the hook itself fails.
	if jh.preProcessor != nil {
		defer jh.preProcessor.release(abnoramlParam)
	} else {
		defer abnoramlParam.content.Close()
	}

	return jh.fn(abnoramlParam.content, abnoramlParam.header)
}

func (jh *judgeHook) Requirements() []string {
	return jh.requireParts
}

type customReadCloser struct {
	io.Reader
	closeFunc func() error
}

func (cc customReadCloser) Close() error {
	return cc.closeFunc()
}
