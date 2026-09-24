package conformance

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/labstack/echo/v4"
	"github.com/mazrean/formstream"
	echoform "github.com/mazrean/formstream/echo"
	ginform "github.com/mazrean/formstream/gin"
	httpform "github.com/mazrean/formstream/http"
	"github.com/mazrean/formstream/internal/pump"
)

// ConformanceBoundary is the fixed boundary used by harness fixtures.
const ConformanceBoundary = "formstream-conformance-boundary"

func quote(s string) string {
	return strconv.Quote(s)
}

func mimeHeader(h map[string][]string) textproto.MIMEHeader {
	return textproto.MIMEHeader(h)
}

// bodyBuilder encodes a scenario to multipart bytes.
func bodyBuilder(sc Scenario) ([]byte, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if err := mw.SetBoundary(ConformanceBoundary); err != nil {
		return nil, err
	}
	for _, part := range sc.Parts {
		if err := writePart(mw, part); err != nil {
			return nil, err
		}
	}
	if err := mw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writePart(mw *multipart.Writer, part PartSpec) error {
	h := make(textproto.MIMEHeader, len(part.Header)+1)
	disposition := "form-data; name=" + quote(part.Name)
	if part.Filename != "" {
		disposition += "; filename=" + quote(part.Filename)
	}
	h.Set("Content-Disposition", disposition)
	if part.ContentType != "" {
		h.Set("Content-Type", part.ContentType)
	}
	for k, v := range part.Header {
		h.Set(k, v)
	}
	pw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	if part.DataReader != nil {
		r, _ := part.DataReader()
		if r != nil {
			defer r.Close()
			if _, err := io.Copy(pw, r); err != nil {
				return err
			}
		}
		return nil
	}
	_, err = pw.Write(part.Data)
	return err
}

// instrumentedBody tracks source reads/closes and optional failure injection.
type instrumentedBody struct {
	r         io.Reader
	reads     *atomic.Int64
	closes    *atomic.Int64
	failAfter int64
	failed    atomic.Bool
	slow      bool
}

func newInstrumentedBody(body []byte, sc Scenario, reads, closes *atomic.Int64) *instrumentedBody {
	b := &instrumentedBody{r: bytes.NewReader(body), reads: reads, closes: closes}
	switch sc.Mutation {
	case BodyFailingReader:
		b.failAfter = int64(len(body) / 2)
	case BodySlow:
		b.slow = true
	}
	return b
}

func (b *instrumentedBody) Read(p []byte) (int, error) {
	if b.slow {
		if len(p) > 64 {
			p = p[:64]
		}
		time.Sleep(20 * time.Microsecond)
	}
	n, err := b.r.Read(p)
	if n > 0 {
		total := b.reads.Add(1)
		if b.failAfter > 0 && !b.failed.Load() && total > b.failAfter {
			b.failed.Store(true)
			return n, ErrReaderFailure
		}
	}
	return n, err
}

func (b *instrumentedBody) Close() error {
	b.closes.Add(1)
	return nil
}

// sink collects observable results from hooks.
type sink struct {
	mu      sync.Mutex
	values  map[string]string
	files   map[string][]fileDelivery
	entered chan struct{}
	enter   sync.Once
}

func newSink() *sink {
	return &sink{values: map[string]string{}, files: map[string][]fileDelivery{}}
}

func (s *sink) addFile(field string, d fileDelivery) {
	s.mu.Lock()
	s.files[field] = append(s.files[field], d)
	s.mu.Unlock()
}

func (s *sink) snapshotFiles() map[string][]fileDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]fileDelivery, len(s.files))
	for k, v := range s.files {
		cp := make([]fileDelivery, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func (s *sink) registerHooks(
	parser interface {
		Register(string, formstream.StreamHookFunc, ...formstream.RegisterOption) error
	},
	sc Scenario,
) error {
	for field, spec := range sc.Hooks {
		field, spec := field, spec
		opts := make([]formstream.RegisterOption, 0, len(spec.RequiredParts))
		for _, req := range spec.RequiredParts {
			opts = append(opts, formstream.WithRequiredPart(req))
		}
		hook := func(r io.Reader, header formstream.Header) error {
			s.enter.Do(func() {
				if s.entered != nil {
					close(s.entered)
				}
			})
			if spec.Delay != nil {
				select {
				case <-spec.Delay:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			var reader io.Reader = r
			if spec.Limit >= 0 {
				reader = io.LimitReader(r, spec.Limit)
			}
			data, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			if header.FileName() != "" {
				s.addFile(field, fileDelivery{
					Filename: header.FileName(),
					Content:  append([]byte(nil), data...),
				})
			}
			return spec.ErrHook
		}
		if err := parser.Register(field, hook, opts...); err != nil {
			return err
		}
	}
	return nil
}

func parserOptions(sc Scenario) []formstream.ParserOption {
	var opts []formstream.ParserOption
	if sc.Options.MaxParts != 0 {
		opts = append(opts, formstream.WithMaxParts(sc.Options.MaxParts))
	}
	if sc.Options.MaxHeaders != 0 {
		opts = append(opts, formstream.WithMaxHeaders(sc.Options.MaxHeaders))
	}
	if sc.Options.MaxMemSize != 0 {
		opts = append(opts, formstream.WithMaxMemSize(sc.Options.MaxMemSize))
	}
	if sc.Options.MaxMemFileSize != 0 {
		opts = append(opts, formstream.WithMaxMemFileSize(sc.Options.MaxMemFileSize))
	}
	return opts
}

func valueKeys(sc Scenario) []string {
	seen := map[string]bool{}
	var keys []string
	for _, p := range sc.Parts {
		if p.Filename == "" && !seen[p.Name] {
			seen[p.Name] = true
			keys = append(keys, p.Name)
		}
	}
	return keys
}

type valueCapturer interface {
	Value(string) (string, formstream.Header, bool)
}

func captureParserValues(p valueCapturer, s *sink, sc Scenario) {
	for _, key := range valueKeys(sc) {
		if v, _, ok := p.Value(key); ok {
			s.values[key] = v
		}
	}
}

// inflightPeak samples the pump via the production sampler hook for the
// duration of one parse. It runs on the same code path as production.
type inflightSampler struct {
	peak atomic.Int64
}

func runCommon(ctx context.Context, sc Scenario, parse func(context.Context, io.Reader) error) (Result, *sink) {
	s := newSink()
	body, err := bodyBuilder(sc)
	if err != nil {
		return Result{Err: err}, s
	}

	var reads, closes atomic.Int64
	br := newInstrumentedBody(body, sc, &reads, &closes)

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	sampler := &inflightSampler{}
	pump.SetSampler(func(v int64) {
		for {
			peak := sampler.peak.Load()
			if v <= peak || sampler.peak.CompareAndSwap(peak, v) {
				break
			}
		}
	})
	defer pump.SetSampler(nil)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if sc.CancelWhen != nil {
		s.entered = make(chan struct{})
		sc.CancelWhen(func() {
			go func() {
				<-s.entered
				cancel()
			}()
		})
	}

	err = parse(ctx, br)

	runtime.ReadMemStats(&memAfter)

	res := Result{
		Err:          err,
		ReadCalls:    reads.Load(),
		CloseCalls:   closes.Load(),
		PeakInflight: sampler.peak.Load(),
		HeapAllocB:   diffHeap(memBefore.HeapAlloc, memAfter.HeapAlloc),
	}
	return res, s
}

func diffHeap(before, after uint64) uint64 {
	if after > before {
		return after - before
	}
	return 0
}

func contentTypeHeader() string {
	return "multipart/form-data; boundary=" + ConformanceBoundary
}

// RootAdapter enters through github.com/mazrean/formstream directly.
type RootAdapter struct{}

func (RootAdapter) Name() string { return "root/formstream" }

func (RootAdapter) Run(ctx context.Context, sc Scenario) Result {
	var parser *formstream.Parser
	var sk *sink
	res, sk := runCommon(ctx, sc, func(ctx context.Context, r io.Reader) error {
		parser = formstream.NewParser(ConformanceBoundary, parserOptions(sc)...)
		if err := sk.registerHooks(parser, sc); err != nil {
			return err
		}
		err := parser.ParseContext(ctx, r)
		if err == nil {
			captureParserValues(parser, sk, sc)
		}
		return err
	})
	res.Values = sk.values
	res.Files = sk.snapshotFiles()
	return res
}

// HTTPAdapter enters through the net/http wrapper.
type HTTPAdapter struct{}

func (HTTPAdapter) Name() string { return "adapter/http" }

func (HTTPAdapter) Run(ctx context.Context, sc Scenario) Result {
	var sk *sink
	res, sk := runCommon(ctx, sc, func(ctx context.Context, r io.Reader) error {
		req := httptest.NewRequest(http.MethodPost, "/upload", r).WithContext(ctx)
		req.Header.Set("Content-Type", contentTypeHeader())
		parser, err := httpform.NewParser(req, parserOptions(sc)...)
		if err != nil {
			return err
		}
		if err := sk.registerHooks(parser, sc); err != nil {
			return err
		}
		err = parser.Parse()
		if err == nil {
			captureParserValues(parser, sk, sc)
		}
		return err
	})
	res.Values = sk.values
	res.Files = sk.snapshotFiles()
	return res
}

// EchoAdapter enters through the Echo wrapper.
type EchoAdapter struct{}

func (EchoAdapter) Name() string { return "adapter/echo" }

func (EchoAdapter) Run(ctx context.Context, sc Scenario) Result {
	e := echo.New()
	var sk *sink
	res, sk := runCommon(ctx, sc, func(ctx context.Context, r io.Reader) error {
		req := httptest.NewRequest(http.MethodPost, "/upload", r).WithContext(ctx)
		req.Header.Set(echo.HeaderContentType, contentTypeHeader())
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		parser, err := echoform.NewParser(c, parserOptions(sc)...)
		if err != nil {
			return err
		}
		if err := sk.registerHooks(parser, sc); err != nil {
			return err
		}
		err = parser.Parse()
		if err == nil {
			captureParserValues(parser, sk, sc)
		}
		return err
	})
	res.Values = sk.values
	res.Files = sk.snapshotFiles()
	return res
}

// GinAdapter enters through the Gin wrapper.
type GinAdapter struct{}

func (GinAdapter) Name() string { return "adapter/gin" }

func (GinAdapter) Run(ctx context.Context, sc Scenario) Result {
	gin.SetMode(gin.ReleaseMode)
	var sk *sink
	res, sk := runCommon(ctx, sc, func(ctx context.Context, r io.Reader) error {
		w := httptest.NewRecorder()
		c := gin.CreateTestContextOnly(w, gin.New())
		c.Request = httptest.NewRequest(http.MethodPost, "/upload", r).WithContext(ctx)
		c.Request.Header.Set("Content-Type", contentTypeHeader())
		parser, err := ginform.NewParser(c, parserOptions(sc)...)
		if err != nil {
			return err
		}
		if err := sk.registerHooks(parser, sc); err != nil {
			return err
		}
		err = parser.Parse()
		if err == nil {
			captureParserValues(parser, sk, sc)
		}
		return err
	})
	res.Values = sk.values
	res.Files = sk.snapshotFiles()
	return res
}

// AllAdapters returns the complete public-adapter matrix.
func AllAdapters() []Adapter {
	return []Adapter{RootAdapter{}, HTTPAdapter{}, EchoAdapter{}, GinAdapter{}}
}
