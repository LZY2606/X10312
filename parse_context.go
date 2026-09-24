package formstream

import (
	"context"
	"errors"
	"io"
)

// Parse is a backward-compatible wrapper around ParseContext using
// context.Background().
func (p *Parser) Parse(r io.Reader) error {
	return p.ParseContext(context.Background(), r)
}

// ParseContext parses the multipart form from r.
//
// The form body is produced on a dedicated goroutine through an in-memory
// pipe so that a slow consumer applies back-pressure to the source reader
// instead of buffering the whole form.
//
// Cancellation semantics:
//   - if ctx is cancelled, parsing stops, the producer goroutine exits and
//     the context cause (context.Canceled by default) is returned;
//   - if r implements io.Closer, it is closed on cancellation so a
//     producer blocked in Read is released;
//   - if a stream hook stops early or returns an error, the producer
//     goroutine exits as well;
//   - the first causal error is preserved: parse/consumer errors take
//     precedence over producer and cancellation errors.
func (p *Parser) ParseContext(ctx context.Context, r io.Reader) (err error) {
	pr, pw := io.Pipe()

	producerDone := make(chan struct{})
	go pipeProducer(pw, r, producerDone)

	watchDone := make(chan struct{})
	go cancelWatcher(ctx, pr, r, producerDone, watchDone)

	hsc := newHookSatisfactionChecker(p.hookMap, &p.parserConfig)
	defer func() {
		deferErr := hsc.Close()
		if deferErr != nil {
			if err != nil {
				err = errors.Join(err, deferErr)
			} else {
				err = deferErr
			}
		}
	}()

	parseErr := p.parse(pr, hsc.IConditionJudger)

	// The consumer is done: release the producer even when the closing
	// boundary was reached while trailing source data is still unread.
	_ = pr.Close()
	<-producerDone
	<-watchDone

	return parseErr
}

// pipeProducer copies the source into the pipe. It is a named function so
// its goroutine stack is attributable to formstream.
func pipeProducer(pw *io.PipeWriter, r io.Reader, done chan<- struct{}) {
	defer close(done)
	_, copyErr := io.Copy(pw, r)
	if copyErr == nil {
		copyErr = io.EOF
	}
	_ = pw.CloseWithError(copyErr)
}

// cancelWatcher unblocks the producer and a blocked source Read when the
// context is cancelled.
func cancelWatcher(ctx context.Context, pr *io.PipeReader, r io.Reader, producerDone <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	select {
	case <-ctx.Done():
		_ = pr.CloseWithError(context.Cause(ctx))
		if c, ok := r.(io.Closer); ok {
			_ = c.Close()
		}
	case <-producerDone:
	}
}
