// Package bufpool provides the buffer pool shared by the formstream
// pre-processing path.
//
// It is a thin wrapper around sync.Pool that additionally tracks outstanding
// buffers. The counters are part of the production package, not a test copy:
// Parse uses exactly this pool for every buffered part, so the conformance
// harness can assert that success results do not carry buffers left behind by
// failed calls.
package bufpool

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// Pool is a buffer pool that tracks the number of buffers currently checked
// out, together with the highest number that was ever outstanding.
type Pool struct {
	pool      sync.Pool
	outgoing  atomic.Int64
	peak      atomic.Int64
	allocated atomic.Int64
}

// New returns a Pool. newBuffer allocates a fresh buffer when the pool is
// empty; in production it is bytes.NewBuffer.
func New(newBuffer func() *bytes.Buffer) *Pool {
	p := &Pool{}
	p.pool.New = func() any {
		p.allocated.Add(1)
		return newBuffer()
	}
	return p
}

// Get removes a buffer from the pool and returns it.
func (p *Pool) Get() *bytes.Buffer {
	buf, ok := p.pool.Get().(*bytes.Buffer)
	if !ok {
		buf = new(bytes.Buffer)
	}
	buf.Reset()

	out := p.outgoing.Add(1)
	for {
		peak := p.peak.Load()
		if out <= peak || p.peak.CompareAndSwap(peak, out) {
			break
		}
	}

	return buf
}

// Put adds the buffer back to the pool.
func (p *Pool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	p.outgoing.Add(-1)
	p.pool.Put(buf)
}

// Stats is a point-in-time snapshot of pool counters.
type Stats struct {
	Outstanding int64
	Peak        int64
	Allocated   int64
}

// Stats returns the current pool counters.
func (p *Pool) Stats() Stats {
	return Stats{
		Outstanding: p.outgoing.Load(),
		Peak:        p.peak.Load(),
		Allocated:   p.allocated.Load(),
	}
}

// Default is the pool used by the parser pre-processing path.
var Default = New(func() *bytes.Buffer { return new(bytes.Buffer) })
