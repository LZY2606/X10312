package conformance

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/mazrean/formstream"
	httpform "github.com/mazrean/formstream/http"
)

// HTTPAdapter enters through the httpform public API.
type HTTPAdapter struct {
	ValueKeys []string
}

// Name implements Adapter.
func (HTTPAdapter) Name() string { return "http" }

// NewParser implements Adapter through httpform.NewParser with a real
// *http.Request. The request carries the boundary in Content-Type and ctx
// as the request context; httpform.Parse runs on that context.
func (a HTTPAdapter) NewParser(ctx context.Context, body io.Reader, options []formstream.ParserOption) (Parser, SnapshotFunc) {
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	p, err := httpform.NewParser(req, options...)
	if err != nil {
		panic(err)
	}
	return adapterShim{register: p.Register, parse: p.Parse}, func() Snapshot {
		return ValuesSnapshot(p.Parser, a.ValueKeys...)
	}
}
