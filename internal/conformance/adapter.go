package conformance

import (
	"context"
	"io"

	"github.com/mazrean/formstream"
)

// ReadSeekCloser is the body interface adapters consume.
type ReadSeekCloser interface {
	io.Reader
	io.Closer
}

// Parser is exactly the production surface the harness drives. The three
// framework adapters embed *formstream.Parser and satisfy it directly; no
// test-only parser implementation exists. Snapshot is taken through the
// public Value API.
type Parser interface {
	Registrar
	ParseContext(ctx context.Context, r io.Reader) error
}

// Registrar is the public hook registration surface.
type Registrar interface {
	Register(name string, fn formstream.StreamHookFunc, options ...formstream.RegisterOption) error
}

// SnapshotFunc reads parser state after Parse returns.
type SnapshotFunc func() Snapshot

// Adapter is a public entry point into formstream.
type Adapter interface {
	// Name identifies the adapter in subtest names and reports.
	Name() string
	// NewParser constructs a parser for one attempt through the adapter's
	// public constructor. Framework adapters install ctx as the request
	// context so their Parse() entry observes cancellation.
	NewParser(ctx context.Context, body io.Reader, options []formstream.ParserOption) (Parser, SnapshotFunc)
}

// Attempt carries the per-run inputs for one adapter execution.
type Attempt struct {
	Body     ReadSeekCloser
	Meter    *Meter
	Recorder *Recorder
	Consumer Consumer
	Options  []formstream.ParserOption
	Ctx      context.Context
}
