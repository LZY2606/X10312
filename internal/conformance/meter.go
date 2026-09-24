package conformance

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Meter is a streaming instrument installed around the body source and the
// readers passed to stream hooks. It tracks read/close counters and the
// peak number of body bytes produced from the source but not yet consumed
// by the parser. Buffering a whole form inflates the peak to the form
// size.
type Meter struct {
	reads        atomic.Int64
	closes       atomic.Int64
	produced     atomic.Int64
	consumed     atomic.Int64
	peakInflight atomic.Int64
}

func (m *Meter) Reads() int64        { return m.reads.Load() }
func (m *Meter) Closes() int64       { return m.closes.Load() }
func (m *Meter) PeakInflight() int64 { return m.peakInflight.Load() }
func (m *Meter) Produced() int64     { return m.produced.Load() }
func (m *Meter) Consumed() int64     { return m.consumed.Load() }

func (m *Meter) producedAdd(n int) {
	if n <= 0 {
		return
	}
	inflight := m.produced.Add(int64(n)) - m.consumed.Load()
	for {
		peak := m.peakInflight.Load()
		if inflight <= peak || m.peakInflight.CompareAndSwap(peak, inflight) {
			break
		}
	}
}

func (m *Meter) consumedAdd(n int) {
	if n > 0 {
		m.consumed.Add(int64(n))
	}
}

// meteredSource wraps the multipart body source. failAfter > 0 makes Read
// fail with ErrReaderFixture once failAfter bytes have been delivered.
type meteredSource struct {
	r         io.Reader
	m         *Meter
	failAfter int64
	delivered int64
	closed    atomic.Bool
}

func (s *meteredSource) Read(p []byte) (int, error) {
	s.m.reads.Add(1)
	n, err := s.r.Read(p)
	if n > 0 {
		s.m.producedAdd(n)
		s.delivered += int64(n)
	}
	if err == nil && s.failAfter > 0 && s.delivered >= s.failAfter {
		err = fmt.Errorf("%w after %d bytes", ErrReaderFixture, s.delivered)
	}
	return n, err
}

func (s *meteredSource) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		s.m.closes.Add(1)
	}
	if c, ok := s.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// consumerReader wraps the reader passed to a stream hook and tracks the
// bytes the consumer drains, so the meter observes the in-flight backlog.
type consumerReader struct {
	r io.Reader
	m *Meter
}

func (c *consumerReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.m.consumedAdd(n)
	return n, err
}
