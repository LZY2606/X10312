package conformance

import (
	"context"
	"io"

	"github.com/mazrean/formstream"
)

// adapterShim adapts a framework parser (whose Parse() consumes the
// request context bound at construction time) to the harness Parser
// interface. The parse closure is the framework adapter's own public Parse
// method, so execution never bypasses its error translation path.
type adapterShim struct {
	register func(name string, fn formstream.StreamHookFunc, options ...formstream.RegisterOption) error
	parse    func() error
}

func (s adapterShim) Register(name string, fn formstream.StreamHookFunc, options ...formstream.RegisterOption) error {
	return s.register(name, fn, options...)
}

func (s adapterShim) ParseContext(_ context.Context, _ io.Reader) error {
	return s.parse()
}
