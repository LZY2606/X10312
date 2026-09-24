package conformance

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/gin-gonic/gin"
	"github.com/mazrean/formstream"
	ginform "github.com/mazrean/formstream/gin"
)

// GinAdapter enters through the ginform public API.
type GinAdapter struct {
	ValueKeys []string
}

// Name implements Adapter.
func (GinAdapter) Name() string { return "gin" }

// NewParser implements Adapter through ginform.NewParser with a real gin
// context and request.
func (a GinAdapter) NewParser(ctx context.Context, body io.Reader, options []formstream.ParserOption) (Parser, SnapshotFunc) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	c.Request = req

	p, err := ginform.NewParser(c, options...)
	if err != nil {
		panic(err)
	}
	return adapterShim{
			register: p.Register,
			parse:    p.Parse,
		}, func() Snapshot {
			return ValuesSnapshot(p.Parser, a.ValueKeys...)
		}
}
