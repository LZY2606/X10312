package formstream

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
)

type partProducer struct {
	parser   *Parser
	hsc      *hookSatisfactionChecker
	mr       *multipart.Reader
	ctx      contextShim
	events   chan *partEvent
	terminal chan error
	cancel   chan struct{}
}

func (pp *partProducer) run() {
	var runErr error
	defer func() {
		pp.terminal <- runErr
		close(pp.events)
	}()

	for {
		if err := pp.ctx.Err(); err != nil {
			runErr = err
			return
		}

		part, err := pp.mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			runErr = fmt.Errorf("failed to read next part: %w", err)
			return
		}

		if pp.parser.maxParts == 0 {
			runErr = ErrTooManyParts
			return
		}
		pp.parser.maxParts--

		for _, header := range part.Header {
			if pp.parser.maxHeaders < uint(len(header)) {
				runErr = ErrTooManyHeaders
				return
			}
			pp.parser.maxHeaders -= uint(len(header))
		}

		header := newHeader(part.Header)
		name := part.FormName()

		switch {
		case pp.hsc.IsSatisfied(name):
			runErr = pp.runLivePart(name, header, part)
		case pp.hsc.IsHookExist(name):
			runErr = pp.runSpooledPart(name, header, part)
		default:
			runErr = pp.runFieldPart(name, header, part)
		}

		if runErr != nil {
			return
		}
	}
}

func (pp *partProducer) runFieldPart(name string, header Header, part *multipart.Part) error {
	if DataSize(len(name)) > pp.parser.maxMemSize {
		return ErrTooLargeForm
	}
	pp.parser.maxMemSize -= DataSize(len(name))

	b := new(bytes.Buffer)
	n, err := io.Copy(b, part)
	if err != nil {
		return fmt.Errorf("failed to copy part: %w", err)
	}

	if uint64(n) > uint64(pp.parser.maxMemSize) {
		return ErrTooLargeForm
	}
	pp.parser.maxMemSize -= DataSize(n)

	return pp.deliver(&partEvent{
		kind:    eventField,
		name:    name,
		header:  header,
		content: b.Bytes(),
		ack:     make(chan struct{}),
	})
}

func (pp *partProducer) runSpooledPart(name string, header Header, part io.Reader) error {
	abnormal, err := pp.hsc.preProcess.run(&normalParam{
		r: part,
		h: header,
	})
	if err != nil {
		return fmt.Errorf("failed to pre-process: %w", err)
	}

	return pp.deliver(&partEvent{
		kind:     eventSpooled,
		name:     name,
		abnormal: abnormal,
		ack:      make(chan struct{}),
	})
}

// runLivePart hands the part reader to the driver (which runs the hook), pumps
// the part body through the bounded channel, and only advances once the driver
// has finished the hook (ack).
func (pp *partProducer) runLivePart(name string, header Header, part io.Reader) error {
	stream := &partStream{
		ctx:    pp.ctx,
		blocks: make(chan *streamBlock, maxInFlightBlocks),
		cancel: pp.cancel,
	}
	ev := &partEvent{
		kind:   eventLive,
		name:   name,
		header: header,
		stream: stream,
		ack:    make(chan struct{}),
	}

	// Send the event first (with back-pressure / cancellation) so the driver
	// can start the hook before any data is pumped.
	select {
	case pp.events <- ev:
	case <-pp.ctx.Done():
		return pp.ctx.Err()
	case <-pp.cancel:
		return errStreamAborted
	}

	pumpErr := pp.pump(part, stream, ev.ack)

	// Wait for the hook to finish before parsing the next part.
	select {
	case <-ev.ack:
	case <-pp.ctx.Done():
		return pp.ctx.Err()
	case <-pp.cancel:
		if pumpErr != nil {
			return pumpErr
		}
		return errStreamAborted
	}

	return pumpErr
}

// deliver sends a non-streaming event and blocks until the driver acks.
func (pp *partProducer) deliver(ev *partEvent) error {
	select {
	case pp.events <- ev:
	case <-pp.ctx.Done():
		return pp.ctx.Err()
	case <-pp.cancel:
		return errStreamAborted
	}

	select {
	case <-ev.ack:
		return nil
	case <-pp.ctx.Done():
		return pp.ctx.Err()
	case <-pp.cancel:
		return errStreamAborted
	}
}

// pump streams src into the bounded block channel.
//
// ackDone is the driver's ack. If the consumer reads only a prefix (or none),
// the block channel fills up; detecting ackDone at that point lets the pump
// drain the remainder of the part instead of blocking forever, keeping the
// multipart reader aligned with the next boundary.
func (pp *partProducer) pump(src io.Reader, stream *partStream, ackDone <-chan struct{}) error {
	stoppedEarly := false

	for {
		block := getBlock()
		n, readErr := src.Read(block.data)
		block.n = n

		if n > 0 {
			if !stoppedEarly {
				select {
				case stream.blocks <- block:
				case <-stream.ctx.Done():
					stoppedEarly = true
					putBlock(block)
				case <-stream.cancel:
					stoppedEarly = true
					putBlock(block)
				case <-ackDone:
					stoppedEarly = true
					putBlock(block)
				}
			} else {
				putBlock(block)
			}
		} else {
			putBlock(block)
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			// A real mid-stream failure (or io.ErrUnexpectedEOF). Surface it
			// through an error block unless the consumer already stopped.
			if !stoppedEarly {
				errBlock := &streamBlock{err: readErr}
				select {
				case stream.blocks <- errBlock:
				case <-stream.ctx.Done():
				case <-stream.cancel:
				case <-ackDone:
				}
			}
			return fmt.Errorf("failed to read part: %w", readErr)
		}

		if stoppedEarly {
			break
		}
	}

	// Consumer stopped early: drain the remainder of the part so the multipart
	// reader stays aligned with the next boundary.
	if stoppedEarly {
		if _, err := io.Copy(io.Discard, src); err != nil {
			return fmt.Errorf("failed to drain part: %w", err)
		}
		return nil
	}

	// All part bytes consumed by the consumer: signal EOF by closing channel.
	close(stream.blocks)
	return nil
}
