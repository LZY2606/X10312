package formstream

import (
	"errors"
	"io"
	"sync"
)

// streamBlockSize is the size of each chunk handed from the producer goroutine
// to the hook consumer. It bounds the amount of part data in flight regardless
// of how slowly the hook reads.
const streamBlockSize = 32 * 1024

// maxInFlightBlocks bounds the total amount of unread part data (2MiB). A
// consumer that stops reading can never force the whole file into memory.
const maxInFlightBlocks = 64

var streamBlockPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, streamBlockSize)
		return &b
	},
}

type streamBlock struct {
	data []byte
	n    int
	err  error
}

// streamContext is the minimal context surface the streaming pipeline needs.
// context.Context satisfies it; accepting the interface keeps the seam small.
type streamContext interface {
	Done() <-chan struct{}
	Err() error
}

// partStream is the io.Reader handed to fast-path hooks. It is fed by the
// producer goroutine through a bounded block channel, so a slow consumer
// applies back-pressure to the source reader instead of buffering the file.
type partStream struct {
	ctx    streamContext
	blocks chan *streamBlock
	// cancel is closed by the driver when parsing must stop (hook error on
	// another part, context cancellation, fatal budget error).
	cancel  chan struct{}
	pending *streamBlock
	off     int
}

func (s *partStream) Read(p []byte) (int, error) {
	if s.pending != nil {
		return s.readPending(p)
	}

	select {
	case block, ok := <-s.blocks:
		if !ok {
			return 0, io.EOF
		}
		if block.err != nil {
			// Producer-side mid-stream read failure; keep the causal error.
			return 0, block.err
		}
		s.pending = block
		s.off = 0
		return s.readPending(p)
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	case <-s.cancel:
		return 0, errStreamAborted
	}
}

func (s *partStream) readPending(p []byte) (int, error) {
	n := copy(p, s.pending.data[s.off:s.pending.n])
	s.off += n
	if s.off == s.pending.n {
		if s.pending.data != nil {
			putBlock(s.pending)
		}
		s.pending = nil
	}
	return n, nil
}

func getBlock() *streamBlock {
	bp := streamBlockPool.Get().(*[]byte)
	return &streamBlock{data: *bp}
}

func putBlock(block *streamBlock) {
	if block == nil {
		return
	}
	bp := block.data[:cap(block.data)]
	streamBlockPool.Put(&bp)
}

// errStreamAborted is the internal control error handed to a hook reader when
// parsing is aborted for a reason unrelated to that reader. It is filtered out
// of the returned error chain in favor of the first causal error.
var errStreamAborted = errors.New("formstream: stream aborted")
