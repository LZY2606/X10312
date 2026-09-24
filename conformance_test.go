package formstream_test

import (
	"context"
	"io"
	"testing"

	"github.com/mazrean/formstream"
	"github.com/mazrean/formstream/internal/conformance"
)

// TestConformance_FormStream_Core drives the cross-adapter conformance and
// leak suite through the core public entry (NewParser/ParseWithContext).
//
// The failure names directly identify the contract
// ("formstream: cross-adapter streaming consistency and leak gate").
func TestConformance_FormStream_Core(t *testing.T) {
	conformance.RunTest(t, func(ct conformance.TestingT) {
		conformance.Run(ct, coreAdapter())
	})
}

func coreAdapter() conformance.Adapter {
	return conformance.Adapter{
		Name: "core",
		NewParser: func(_ conformance.TestingT, body io.Reader, _ string, options ...formstream.ParserOption) conformance.Parser {
			// The core parser takes the boundary explicitly; every harness
			// body uses conformance.Boundary.
			return &coreParser{
				Parser: formstream.NewParser(conformance.Boundary, options...),
				r:      body,
			}
		},
	}
}

type coreParser struct {
	*formstream.Parser
	r io.Reader
}

func (p *coreParser) ParseWithContext(ctx context.Context) error {
	return p.Parser.ParseWithContext(ctx, p.r)
}
