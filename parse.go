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

// Parse parses the multipart form from r without a cancellation context.
// Prefer [Parser.ParseContext] when the caller already has a context.
func (p *Parser) Parse(r io.Reader) error {
	return p.ParseContext(context.Background(), r)
}

// ParseContext parses the multipart form from r.
//
// Part bodies of fast-path hooks are streamed through a bounded buffer from a
// producer goroutine: a slow consumer back-pressures the source reader instead
// of buffering the whole file, and a consumer that reads only a prefix never
// blocks the producer. Cancelling ctx, a failing source reader or a failing hook
// all terminate the producer; the first causal error is preserved.
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

	return p.parse(ctx, r, hsc)
}

type partEventKind int

const (
	eventField partEventKind = iota
	eventSpooled
	eventLive
)

// partEvent is a hand-off from the producer goroutine to the driver. Every
// event carries an ack channel the driver closes after the event (hook and/or
// KeyEvent) is handled, preserving the original sequential ordering.
type partEvent struct {
	kind   partEventKind
	name   string
	header Header

	// field
	content []byte

	// spooled (abnormal path)
	abnormal *abnormalParam

	// live (fast path)
	stream *partStream

	ack chan struct{}
}

// parse drives one producer goroutine (all multipart.Reader I/O, spooling and
// budget accounting) and runs every hook on the caller goroutine. This is the
// single execution path shared by the public adapters.
func (p *Parser) parse(ctx context.Context, r io.Reader, hsc *hookSatisfactionChecker) error {
	mr := multipart.NewReader(r, p.boundary)

	events := make(chan *partEvent, 2)
	terminal := make(chan error, 1)
	cancel := make(chan struct{})
	var cancelOnce sync.Once
	abort := func() { cancelOnce.Do(func() { close(cancel) }) }

	producer := &partProducer{
		parser:   p,
		hsc:      hsc,
		mr:       mr,
		ctx:      ctx,
		events:   events,
		terminal: terminal,
		cancel:   cancel,
	}
	go producer.run()

	var firstErr error
	setErr := func(err error) {
		if err != nil && firstErr == nil && !errors.Is(err, errStreamAborted) {
			firstErr = err
		}
	}

driverLoop:
	for {
		select {
		case <-ctx.Done():
			setErr(ctx.Err())
			abort()
			break driverLoop
		case ev := <-events:
			if ev == nil {
				break driverLoop
			}

			handleErr := p.handleEvent(hsc.IConditionJudger, ev)
			if handleErr != nil {
				setErr(handleErr)
				abort()
			}
			close(ev.ack)

			if handleErr != nil {
				break driverLoop
			}
		case err := <-terminal:
			setErr(err)
			break driverLoop
		}
	}

	// Drain queued events until the producer has exited, so it can never block
	// on a full channel and leak a goroutine.
	eventsOpen := true
	terminalOpen := true
	for eventsOpen || terminalOpen {
		select {
		case ev := <-events:
			if ev == nil {
				eventsOpen = false
			} else {
				close(ev.ack)
			}
		case err := <-terminal:
			setErr(err)
			terminalOpen = false
		}
}

// handleEvent runs the exact public-facing side effect of a part on the
// caller goroutine: storing a field, replaying a spooled hook, or executing a
// fast-path hook followed by KeyEvent.
func (p *Parser) handleEvent(hsc conditionjudge.IConditionJudger[string, *normalParam, *abnormalParam], ev *partEvent) error {
	switch ev.kind {
	case eventField:
		p.valueMu.Lock()
		p.valueMap[ev.name] = append(p.valueMap[ev.name], Value{
			content: ev.content,
			header:  ev.header,
		})
		p.valueMu.Unlock()

		if err := hsc.KeyEvent(ev.name); err != nil {
			return fmt.Errorf("failed to run satisfied hook: %w", err)
		}
	case eventSpooled:
		if _, err := hsc.HookEvent(ev.name, nil); err != nil {
			return fmt.Errorf("failed to run or set hook: %w", err)
		}
		if err := hsc.KeyEvent(ev.name); err != nil {
			return fmt.Errorf("failed to run satisfied hook: %w", err)
		}
	case eventLive:
		if _, err := hsc.HookEvent(ev.name, &normalParam{
			r: ev.stream,
			h: ev.header,
		}); err != nil {
			return fmt.Errorf("failed to run or set hook: %w", err)
		}
		if err := hsc.KeyEvent(ev.name); err != nil {
			return fmt.Errorf("failed to run satisfied hook: %w", err)
		}
	}

	return nil
}
