package conformance

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/gin-gonic/gin"
	"github.com/labstack/echo/v4"
	"github.com/mazrean/formstream"
	echoform "github.com/mazrean/formstream/echo"
	ginform "github.com/mazrean/formstream/gin"
	httpform "github.com/mazrean/formstream/http"
)

func contentType() string { return "multipart/form-data; boundary=" + Boundary }

func newRequest(source io.ReadCloser) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/", source)
	req.Header.Set("Content-Type", contentType())
	return req
}

func valueView(parser *formstream.Parser, key string) (string, HeaderView, bool) {
	v, h, ok := parser.Value(key)
	return v, HeaderView{Name: h.Name(), Filename: h.FileName(), ContentType: h.ContentType()}, ok
}

// registerAll wires the case hooks onto a parser through the public API.
func registerAll(parser *formstream.Parser, source io.ReadCloser, hooks []*HookSpec) {
	reg := parser.Register
	for _, h := range hooks {
		h.register(reg, source.(*trackedReadCloser), h.files)
	}
}

// CoreAdapter enters through formstream.NewParser directly.
type CoreAdapter struct{}

func (CoreAdapter) Name() string { return "formstream/core" }

func (CoreAdapter) NewParser(source io.ReadCloser, hooks []*HookSpec, opts []formstream.ParserOption) (ParseCall, ParserValues) {
	parser := formstream.NewParser(Boundary, opts...)
	registerAll(parser, source, hooks)
	return func(ctx context.Context) error { return parser.ParseContext(ctx, source) },
		func(key string) (string, HeaderView, bool) { return valueView(parser, key) }
}

// HTTPAdapter enters through httpform.NewParser.
type HTTPAdapter struct{}

func (HTTPAdapter) Name() string { return "httpform" }

func (HTTPAdapter) NewParser(source io.ReadCloser, hooks []*HookSpec, opts []formstream.ParserOption) (ParseCall, ParserValues) {
	req := newRequest(source)
	parser, err := httpform.NewParser(req, opts...)
	if err != nil {
		panic(err)
	}
	registerAll(parser.Parser, source, hooks)
	return func(ctx context.Context) error { return parser.ParseContext(ctx) },
		func(key string) (string, HeaderView, bool) { return valueView(parser.Parser, key) }
}

// EchoAdapter enters through echoform.NewParser.
type EchoAdapter struct{}

func (EchoAdapter) Name() string { return "echoform" }

func (EchoAdapter) NewParser(source io.ReadCloser, hooks []*HookSpec, opts []formstream.ParserOption) (ParseCall, ParserValues) {
	e := echo.New()
	req := newRequest(source)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	parser, err := echoform.NewParser(c, opts...)
	if err != nil {
		panic(err)
	}
	registerAll(parser.Parser, source, hooks)
	return func(ctx context.Context) error { return parser.ParseContext(ctx) },
		func(key string) (string, HeaderView, bool) { return valueView(parser.Parser, key) }
}

// GinAdapter enters through ginform.NewParser.
type GinAdapter struct{}

func ginInit() { gin.SetMode(gin.TestMode) }

func (GinAdapter) Name() string { return "ginform" }

func (GinAdapter) NewParser(source io.ReadCloser, hooks []*HookSpec, opts []formstream.ParserOption) (ParseCall, ParserValues) {
	ginInit()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = newRequest(source)
	parser, err := ginform.NewParser(c, opts...)
	if err != nil {
		panic(err)
	}
	registerAll(parser.Parser, source, hooks)
	return func(ctx context.Context) error { return parser.ParseContext(ctx) },
		func(key string) (string, HeaderView, bool) { return valueView(parser.Parser, key) }
}

// Adapters is the canonical cross-adapter matrix.
func Adapters() []Adapter {
	return []Adapter{CoreAdapter{}, HTTPAdapter{}, EchoAdapter{}, GinAdapter{}}
}
