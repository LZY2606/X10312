// Package conformance defines the shared "formstream: cross-adapter streaming
// conformance and leak gate" harness.
//
// Every scenario enters formstream exclusively through its official public
// entry points (formstream.NewParser / the adapter NewParser constructors) and
// registers hooks only via Parser.Register. The harness never fabricates the
// package's internal parser or buffered objects: it drives the real execution
// path and only observes results and resource counters.
package conformance

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/mazrean/formstream"
)

// Boundary is the fixed multipart boundary used by every conformance scenario.
const Boundary = "formstream-conformance-boundary"

// FileSpec describes one file part sent in a scenario.
type FileSpec struct {
	Name     string
	Filename string
	Content  []byte
}

// Collector records what the real hook functions observe through the public
// Header and io.ReadCloser API. It is shared with the scenario's Register
// callback and protected for scenarios whose requirements replay late.
type Collector struct {
	mu     sync.Mutex
	Files  []FileRecord
	Values []ValueRecord
}

// FileRecord is one file delivered to a registered hook.
type FileRecord struct {
	Field       string
	Filename    string
	ContentType string
	Size        int
	SHA256      string
}

// ValueRecord is one non-hook field as observed through Parser.Value.
type ValueRecord struct {
	Field   string
	Content string
}

// Register is the callback each test hands to Run so it can wire hooks on the
// parser built by the public adapter constructor.
type Register func(p *formstream.Parser, c *Collector, opts ...formstream.RegisterOption) error

// Adapter is the narrow seam a public adapter package implements to drive its
// official constructor. Setup must call NewParser for the framework (or
// formstream.NewParser for the core adapter) and return the resulting parser
// together with a parse function bound to the given body.
type Adapter struct {
	Name  string
	Setup func(t *testing.T, body io.Reader, options ...formstream.ParserOption) (parser *formstream.Parser, parse func() error)
}

// RunResult summarizes one execution of a scenario for diagnostics.
type RunResult struct {
	Stats
	Err       error
	Collector *Collector
}

// Stats are observable resource counters attached to the scenario body.
type Stats struct {
	ReadCalls  int64
	ReadBytes  int64
	CloseCalls int64
	// PeakInflight is only populated by streaming scenarios: the largest number
	// of body bytes written but not yet consumed by a hook at any instant.
	PeakInflight int64
}

// Want describes the expected caller-visible outcome of a scenario.
type Want struct {
	// WantErr reports whether the public Parse entry must return an error.
	WantErr bool
	// ErrIs matches the first causal error with errors.Is (nil disables).
	ErrIs error
	// Fields are the field values expected via Parser.Value after success.
	Fields map[string]string
	// Files are the files expected at hooks, in delivery order.
	Files []FileRecord
}

// Parse runs one scenario through a public adapter and returns the observed
// result. registerHooks installs the scenario's hooks through Register.
func Parse(t *testing.T, adapter Adapter, s Scenario, registerHooks func(t *testing.T, p *formstream.Parser, c *Collector)) RunResult {
	t.Helper()

	body, stats, cancel := s.BuildBody()
	defer cancel()

	options := s.Options
	if options == nil {
		options = []formstream.ParserOption{}
	}
	parser, parse := adapter.Setup(t, body, options...)

	c := &Collector{}
	registerHooks(t, parser, c)

	err := parse()

	return RunResult{Err: err, Collector: c, Stats: stats.snapshot()}
}

// Assert checks caller-visible semantics against want and returns a
// descriptive failure that names the first violated invariant.
func Assert(t *testing.T, adapterName, scenarioName string, res RunResult, want Want) {
	t.Helper()

	if want.WantErr {
		if res.Err == nil {
			t.Fatalf("[%s/%s] expected Parse to report an error, got nil; stats=%+v files=%d fields=%d",
				adapterName, scenarioName, res.Stats, len(res.Collector.Files), len(res.Collector.Values))
		}
		if want.ErrIs != nil && !errorIs(res.Err, want.ErrIs) {
			t.Fatalf("[%s/%s] first causal error mismatch: want errors.Is(%v), got %v; stats=%+v",
				adapterName, scenarioName, want.ErrIs, res.Err, res.Stats)
		}
		return
	}

	if res.Err != nil {
		t.Fatalf("[%s/%s] expected success, got error: %v; stats=%+v",
			adapterName, scenarioName, res.Err, res.Stats)
	}

	// Fields observed through the public Parser.Value API.
	for field, wantValue := range want.Fields {
		got, ok := res.Collector.lookupValue(field)
		if !ok {
			t.Fatalf("[%s/%s] missing expected field %q after success; stats=%+v",
				adapterName, scenarioName, field, res.Stats)
		}
		if got != wantValue {
			t.Fatalf("[%s/%s] field %q mismatch: want %q, got %q",
				adapterName, scenarioName, field, wantValue, got)
		}
	}

	if len(res.Collector.Files) != len(want.Files) {
		t.Fatalf("[%s/%s] file count mismatch: want %d, got %d (%v)",
			adapterName, scenarioName, len(want.Files), len(res.Collector.Files), res.Collector.Files)
	}
	for i, wantFile := range want.Files {
		got := res.Collector.Files[i]
		if got != wantFile {
			t.Fatalf("[%s/%s] file[%d] mismatch: want %+v, got %+v",
				adapterName, scenarioName, i, wantFile, got)
		}
	}
}

func errorIs(err, target error) bool {
	return errors.Is(err, target)
}

func (c *Collector) lookupValue(field string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.Values {
		if v.Field == field {
			return v.Content, true
		}
	}
	return "", false
}

// ConsumeHook is the standard hook implementation: it fully drains the part
// through the public io.Reader and records a FileRecord.
func ConsumeHook(c *Collector) formstream.StreamHookFunc {
	return func(r io.Reader, header formstream.Header) error {
		h := sha256.New()
		n, err := io.Copy(h, r)
		if err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.Files = append(c.Files, FileRecord{
			Field:       header.Name(),
			Filename:    header.FileName(),
			ContentType: header.ContentType(),
			Size:        int(n),
			SHA256:      hex.EncodeToString(h.Sum(nil)),
		})
		return nil
	}
}

// ReadFields records every value visible via Parser.Value into the collector.
// It is called after a successful Parse.
func ReadFields(p *formstream.Parser, c *Collector, fields ...string) {
	for _, field := range fields {
		v, _, ok := p.Value(field)
		if ok {
			c.mu.Lock()
			c.Values = append(c.Values, ValueRecord{Field: field, Content: v})
			c.mu.Unlock()
		}
	}
}
