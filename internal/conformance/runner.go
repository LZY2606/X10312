package conformance

import (
	"context"
	"testing"

	"github.com/mazrean/formstream"
)

// ParseOptions are parser-limit options for one attempt. Zero values fall
// back to the library defaults.
type ParseOptions struct {
	MaxParts       uint
	MaxHeaders     uint
	MaxMemSize     formstream.DataSize
	MaxMemFileSize formstream.DataSize
}

func (o ParseOptions) toOptions() []formstream.ParserOption {
	var opts []formstream.ParserOption
	if o.MaxParts != 0 {
		opts = append(opts, formstream.WithMaxParts(o.MaxParts))
	}
	if o.MaxHeaders != 0 {
		opts = append(opts, formstream.WithMaxHeaders(o.MaxHeaders))
	}
	if o.MaxMemSize != 0 {
		opts = append(opts, formstream.WithMaxMemSize(o.MaxMemSize))
	}
	if o.MaxMemFileSize != 0 {
		opts = append(opts, formstream.WithMaxMemFileSize(o.MaxMemFileSize))
	}
	return opts
}

// ReaderFault injects a recoverable reader failure.
type ReaderFault struct {
	// FailAfterBytes triggers ErrReaderFixture after this many body bytes.
	FailAfterBytes int64
	// Attempt is the 1-based attempt number on which the fault fires; 0
	// means every attempt.
	Attempt int
}

// AttemptConfig configures one execution of a case.
type AttemptConfig struct {
	Form     FormSpec
	Consumer Consumer
	Options  ParseOptions
	// Cancel, when non-nil, is invoked once the first file hook fires and
	// must cancel the context it receives. The cancelled context becomes
	// the request context for framework adapters.
	Cancel      func(cancel context.CancelCauseFunc)
	ReaderFault *ReaderFault
}

// AttemptResult reports observable and internal-invariant metrics.
type AttemptResult struct {
	Err      error
	Class    ErrorClass
	Snapshot Snapshot
	Files    []FileCall
	Meter    *Meter
}

// RunCase executes one attempt through the given public adapter and
// registers every declared file hook through the public Register API.
func RunCase(t testing.TB, adapter Adapter, attemptNo int, cfg AttemptConfig) AttemptResult {
	t.Helper()

	formFile, formSize, err := BuildFile(cfg.Form)
	if err != nil {
		t.Fatalf("BuildFile: %v", err)
	}
	t.Cleanup(func() {
		_ = formFile.Close()
	})

	meter := &Meter{}
	rec := &Recorder{}

	failAfter := int64(0)
	if cfg.ReaderFault != nil && (cfg.ReaderFault.Attempt == 0 || cfg.ReaderFault.Attempt == attemptNo) {
		failAfter = cfg.ReaderFault.FailAfterBytes
	}
	body := &meteredSource{r: formFile, m: meter, failAfter: failAfter}

	ctx := context.Background()
	var cancel context.CancelCauseFunc = func(error) {}
	if cfg.Cancel != nil {
		ctx, cancel = context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(context.Canceled) })
	}

	parser, snapshot := adapter.NewParser(ctx, body, cfg.Options.toOptions())
	hook := NewHook(meter, rec, cfg.Consumer)
	for field, spec := range cfg.Consumer.Files {
		var regOpts []formstream.RegisterOption
		for _, req := range spec.Requirements {
			regOpts = append(regOpts, formstream.WithRequiredPart(req))
		}
		if err := parser.Register(field, hook, regOpts...); err != nil {
			t.Fatalf("Register(%q): %v", field, err)
		}
	}

	if cfg.Cancel != nil {
		go func() {
			rec.WaitForFiles(1)
			cfg.Cancel(cancel)
		}()
	}

	parseErr := parser.ParseContext(ctx, body)
	closeErr := body.Close()
	if parseErr == nil && closeErr != nil {
		parseErr = closeErr
	}

	result := AttemptResult{
		Err:      parseErr,
		Class:    Classify(parseErr),
		Snapshot: snapshot(),
		Files:    rec.Files(),
		Meter:    meter,
	}

	t.Logf("adapter=%s attempt=%d formSize=%d class=%s reads=%d closes=%d produced=%d consumed=%d peakInflight=%d files=%d",
		adapter.Name(), attemptNo, formSize, result.Class, meter.Reads(), meter.Closes(),
		meter.Produced(), meter.Consumed(), meter.PeakInflight(), len(result.Files))

	return result
}
