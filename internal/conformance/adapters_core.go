package conformance

import (
	"context"
	"io"

	"github.com/mazrean/formstream"
)

// CoreAdapter enters through the package-level formstream public API.
type CoreAdapter struct {
	// ValueKeys are snapshotted via Parser.Value after parsing.
	ValueKeys []string
}

// Name implements Adapter.
func (CoreAdapter) Name() string { return "core" }

// NewParser implements Adapter through formstream.NewParser.
func (a CoreAdapter) NewParser(ctx context.Context, body io.Reader, options []formstream.ParserOption) (Parser, SnapshotFunc) {
	_ = ctx
	p := formstream.NewParser(boundary, options...)
	return p, func() Snapshot {
		return ValuesSnapshot(p, a.ValueKeys...)
	}
}
