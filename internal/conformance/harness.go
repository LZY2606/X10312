// Package conformance provides the shared, adapter-agnostic conformance
// harness for "formstream: cross-adapter streaming consistency and leak
// gating".
//
// Every check enters production code through the same public entry points
// users use (formstream.NewParser/Register/ParseContext and the
// http/echo/gin adapter NewParser/Parse). The harness never fabricates
// internal parser objects: adapters only expose how they bind stream hooks
// through the public registration API and how parser values are snapshotted
// after parsing completes.
//
// The instrumentation wraps the request body and the io.Reader handed to
// stream hooks. It records read/close counters and the peak number of
// produced-but-unconsumed body bytes. No code path buffers the complete
// body: large forms are streamed from a temporary file and consumed chunk
// by chunk.
package conformance

import (
	"errors"
	"sync"
)

const boundary = "formstream-conformance-boundary"

// Sentinel errors used as deliberate, recoverable failures injected by the
// fixtures. Adapters surface them through the public hook error path.
var (
	ErrConsumerFixture = errors.New("conformance: injected consumer failure")
	ErrReaderFixture   = errors.New("conformance: injected reader failure")
)

// FileCall is one observation of a stream hook invocation.
type FileCall struct {
	Field       string `json:"field"`
	FileName    string `json:"file_name"`
	ContentType string `json:"content_type"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
	Order       int    `json:"order"`
}

// Recorder collects observable outcomes of one parse attempt. It is safe
// for concurrent use because stream hooks run while the producer goroutine
// may still be active.
type Recorder struct {
	mu      sync.Mutex
	files   []FileCall
	order   int
	changed chan struct{}
}

// Record adds a hook observation and returns its 1-based order.
func (r *Recorder) Record(field, fileName, contentType string, n int64, sum string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order++
	r.files = append(r.files, FileCall{
		Field:       field,
		FileName:    fileName,
		ContentType: contentType,
		Bytes:       n,
		SHA256:      sum,
		Order:       r.order,
	})
	if r.changed != nil {
		close(r.changed)
		r.changed = nil
	}
	return r.order
}

// WaitForFiles blocks until at least n hook calls have been recorded.
func (r *Recorder) WaitForFiles(n int) {
	for {
		r.mu.Lock()
		count := len(r.files)
		ch := r.changed
		if ch == nil {
			ch = make(chan struct{})
			r.changed = ch
		}
		r.mu.Unlock()
		if count >= n {
			return
		}
		<-ch
	}
}

// Files returns a copy of the hook observations in invocation order.
func (r *Recorder) Files() []FileCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]FileCall, len(r.files))
	copy(out, r.files)
	return out
}
