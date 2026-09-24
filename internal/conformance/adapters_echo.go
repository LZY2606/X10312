package conformance

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/labstack/echo/v4"
	"github.com/mazrean/formstream"
	echoform "github.com/mazrean/formstream/echo"
)

// EchoAdapter enters through the echoform public API.
type EchoAdapter struct {
	ValueKeys []string
}

// Name implements Adapter.
func (EchoAdapter) Name() string { return "echo" }

// NewParser implements Adapter through echoform.NewParser with a real echo
// context and request.
func (a EchoAdapter) NewParser(ctx context.Context, body io.Reader, options []formstream.ParserOption) (Parser, SnapshotFunc) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req = req.WithContext(ctx)
	req.Header.Set(echo.HeaderContentType, "multipart/form-data; boundary="+boundary)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	p, err := echoform.NewParser(c, options...)
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
