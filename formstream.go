package formstream

import (
	"context"
	"io"
	"mime"
	"net/textproto"
	"sync"
)

type Parser struct {
	boundary string
	valueMap map[string][]Value
	valueMu  sync.RWMutex
	hookMap  map[string]streamHook
	parserConfig
}

func NewParser(boundary string, options ...ParserOption) *Parser {
	c := parserConfig{
		maxParts:       defaultMaxParts,
		maxHeaders:     defaultMaxHeaders,
		maxMemSize:     defaultMaxMemSize,
		maxMemFileSize: defaultMaxMemFileSize,
	}
	for _, opt := range options {
		opt(&c)
	}

	return &Parser{
		boundary:     boundary,
		valueMap:     make(map[string][]Value),
		hookMap:      make(map[string]streamHook),
		parserConfig: c,
	}
}

type parserConfig struct {
	maxParts       uint
	maxHeaders     uint
	maxMemSize     DataSize
	maxMemFileSize DataSize
	ctx            context.Context
}

type ParserOption func(*parserConfig)

type DataSize int64

const (
	_ DataSize = 1 << (iota * 10)
	KB
	MB
	GB
)

const (
	defaultMaxParts       = 10000
	defaultMaxHeaders     = 10000
	defaultMaxMemSize     = 32 * MB
	defaultMaxMemFileSize = 32 * MB
)

// WithMaxParts sets the maximum number of parts to be parsed.
// default: 10000
func WithMaxParts(maxParts uint) ParserOption {
	return func(c *parserConfig) {
		c.maxParts = maxParts
	}
}

// WithMaxHeaders sets the maximum number of headers to be parsed.
// default: 10000
func WithMaxHeaders(maxHeaders uint) ParserOption {
	return func(c *parserConfig) {
		c.maxHeaders = maxHeaders
	}
}

// WithMaxMemSize sets the maximum memory size to be used for parsing.
// default: 32MB
func WithMaxMemSize(maxMemSize DataSize) ParserOption {
	return func(c *parserConfig) {
		c.maxMemSize = maxMemSize
	}
}

// WithMaxMemFileSize sets the maximum memory size to be used for parsing a file.
// default: 32MB
func WithMaxMemFileSize(maxMemFileSize DataSize) ParserOption {
	return func(c *parserConfig) {
		c.maxMemFileSize = maxMemFileSize
	}
}

// WithContext enables context-aware streaming parsing.
//
// Without it (or with a nil context) parsing stays on the single calling
// goroutine exactly as before. With a non-nil context the part pump runs on a
// producer goroutine and registered hooks run on the calling goroutine, so the
// body is read with bounded in-flight buffering and cancellation propagates to
// the producer. Cancelling ctx makes Parse return the context error (wrapped)
// and guarantees the producer goroutine exits.
func WithContext(ctx context.Context) ParserOption {
	return func(c *parserConfig) {
		c.ctx = ctx
	}
}

type Value struct {
	content []byte
	header  Header
}

// Unwrap returns the content and header of the value.
func (v Value) Unwrap() (string, Header) {
	return string(v.content), v.header
}

// UnwrapRaw returns the raw content and header of the value.
func (v Value) UnwrapRaw() ([]byte, Header) {
	return v.content, v.header
}

type Header struct {
	dispositionParams map[string]string
	header            textproto.MIMEHeader
}

func newHeader(h textproto.MIMEHeader) Header {
	contentDisposition := h.Get("Content-Disposition")
	_, params, err := mime.ParseMediaType(contentDisposition)
	if err != nil {
		params = make(map[string]string)
	}

	return Header{
		dispositionParams: params,
		header:            h,
	}
}

// Get returns the first value associated with the given key.
// If there are no values associated with the key, Get returns "".
func (h Header) Get(key string) string {
	return h.header.Get(key)
}

// ContentType returns the value of the "Content-Type" header field.
// If there are no values associated with the key, ContentType returns "".
func (h Header) ContentType() string {
	return h.header.Get("Content-Type")
}

// Name returns the value of the "name" parameter in the "Content-Disposition" header field.
// If there are no values associated with the key, Name returns "".
func (h Header) Name() string {
	return h.dispositionParams["name"]
}

// FileName returns the value of the "filename" parameter in the "Content-Disposition" header field.
// If there are no values associated with the key, FileName returns "".
func (h Header) FileName() string {
	return h.dispositionParams["filename"]
}

type StreamHookFunc = func(r io.Reader, header Header) error

type streamHook struct {
	fn           StreamHookFunc
	requireParts []string
}
