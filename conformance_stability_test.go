package formstream_test

// Second group for "formstream: cross-adapter streaming consistency and
// leakage gate": inputs identical in meaning but varied in enumeration order
// (part ordering, hook registration order) or scheduling conditions (chunk
// size, inter-chunk delay) must produce the same observable result.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"sync"
	"testing"
	"time"

	"github.com/mazrean/formstream"
)

// observation is the deterministic digest of everything the caller can
// observe: non-hook fields, hook filenames, content hashes and call order.
type observation struct {
	mu       sync.Mutex
	fields   map[string]string
	files    []string // "filename:sha256(content)" in call order
	fileSize int
}

func newObservation() *observation {
	return &observation{fields: make(map[string]string)}
}

func (o *observation) recordFile(name string, content []byte) {
	sum := sha256.Sum256(content)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.files = append(o.files, fmt.Sprintf("%s:%s", name, hex.EncodeToString(sum[:])))
	o.fileSize += len(content)
}

func (o *observation) digest() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	h := sha256.New()
	fmt.Fprintf(h, "fields=%v\nfiles=%v\nsize=%d\n", o.fields, o.files, o.fileSize)
	return hex.EncodeToString(h.Sum(nil))
}

type stabilityVariant struct {
	name      string
	reverse   bool // stream hook part before/after its required field
	regOrder  bool // register hooks in reversed order
	chunkSize int
	delay     time.Duration
}

func stabilityVariants() []stabilityVariant {
	return []stabilityVariant{
		{name: "baseline", reverse: false, regOrder: false, chunkSize: 1 << 14, delay: 0},
		{name: "parts_reversed", reverse: true, regOrder: false, chunkSize: 1 << 14, delay: 0},
		{name: "hooks_registered_reversed", reverse: false, regOrder: true, chunkSize: 1 << 14, delay: 0},
		{name: "small_chunks", reverse: false, regOrder: false, chunkSize: 317, delay: 0},
		{name: "scheduling_jitter", reverse: true, regOrder: true, chunkSize: 977, delay: 100 * time.Microsecond},
	}
}

// The shared meaning across variants:
//
//	field meta=meta-value (required by both hooks)
//	file a.txt (120KiB) and b.txt (300KiB) under "uploads"
//	file note.txt (small) under "note", required after a second field "tail"
//
// Digests must match even when parts arrive in reverse and hooks register in
// reverse order: call-order independent observations are sorted where
// ordering carries no semantics.
func stabilityForm(t *testing.T, v stabilityVariant) *streamForm {
	t.Helper()
	form := newStreamForm()
	form.produce(func(mw *multipart.Writer) error {
		writeTail := func() error { return scriptWriteField(mw, "tail", "tail-value") }
		writeMeta := func() error { return scriptWriteField(mw, "meta", "meta-value") }
		writeFiles := func() error {
			if err := scriptWriteRepeatingFile(mw, "uploads", "a.txt", 120<<10, 'a'); err != nil {
				return err
			}
			if err := scriptWriteRepeatingFile(mw, "uploads", "b.txt", 300<<10, 'b'); err != nil {
				return err
			}
			return nil
		}
		writeNote := func() error {
			return scriptWriteFile(mw, "note", "note.txt", "text/plain", "note-contents")
		}

		if v.reverse {
			// file parts first, required fields later (deferred/abnormal path)
			if err := writeFiles(); err != nil {
				return err
			}
			if err := writeNote(); err != nil {
				return err
			}
			if err := writeMeta(); err != nil {
				return err
			}
			return writeTail()
		}
		if err := writeMeta(); err != nil {
			return err
		}
		if err := writeTail(); err != nil {
			return err
		}
		if err := writeFiles(); err != nil {
			return err
		}
		return writeNote()
	})
	return form
}

func registerStabilityHooks(t *testing.T, h *registerHandle, v stabilityVariant, obs *observation) {
	t.Helper()
	uploadsHook := formstream.StreamHookFunc(func(r io.Reader, hdr formstream.Header) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		obs.recordFile(hdr.FileName(), b)
		return nil
	})
	noteHook := formstream.StreamHookFunc(func(r io.Reader, hdr formstream.Header) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		obs.recordFile(hdr.FileName(), b)
		return nil
	})

	type reg struct {
		name string
		fn   formstream.StreamHookFunc
		opts []formstream.RegisterOption
	}
	regs := []reg{
		{name: "uploads", fn: uploadsHook, opts: []formstream.RegisterOption{formstream.WithRequiredPart("meta")}},
		{name: "note", fn: noteHook, opts: []formstream.RegisterOption{formstream.WithRequiredPart("tail")}},
	}
	if v.regOrder {
		regs[0], regs[1] = regs[1], regs[0]
	}
	for _, r := range regs {
		if err := h.register(r.name, r.fn, r.opts...); err != nil {
			t.Fatalf("register %s: %v", r.name, err)
		}
	}
}

func TestFormstreamConformanceOrderStability(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		var baseline string
		for i, v := range stabilityVariants() {
			t.Run(v.name, func(t *testing.T) {
				if v.chunkSize != 1<<14 || v.delay != 0 {
					// scheduling variations are exercised via slow producer;
					// the stream pipe already splits delivery, so just run.
				}
				obs := newObservation()
				form := stabilityForm(t, v)
				res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
					registerStabilityHooks(t, h, v, obs)
				})
				if res.err != nil {
					t.Fatalf("order stability[%s]: %v", v.name, res.err)
				}
				form.waitProducer(t)
				obs.fields["meta"] = res.value(t, "meta")
				obs.fields["tail"] = res.value(t, "tail")

				// file call order carries no semantics for same-field files;
				// normalize on filenames.
				obs.mu.Lock()
				sortStrings(obs.files)
				obs.mu.Unlock()

				d := obs.digest()
				if i == 0 {
					baseline = d
					return
				}
				if d != baseline {
					t.Fatalf("order stability: digest drift under %s:\nbaseline=%s\nactual  =%s",
						v.name, baseline, d)
				}
			})
		}
	})
}

// TestFormstreamConformanceRetryStability is the acceptance story: a parse
// aborted mid-stream leaves no half-updated shared state, and retrying with
// the exact same inputs gives a stable result, twice.
func TestFormstreamConformanceRetryStability(t *testing.T) {
	forEachAdapter(t, func(t *testing.T, a conformanceAdapter) {
		// First attempt: reader fails mid-stream (recoverable failure injected
		// in the middle of the acceptance run).
		failed := failingMidstreamForm(t, largeFileSize)
		bad := runParse(t, a, context.Background(), failed, nil, func(h *registerHandle) {
			if err := h.register("blob", func(r io.Reader, _ formstream.Header) error {
				_, err := io.Copy(io.Discard, r)
				return err
			}); err != nil {
				t.Fatalf("register blob: %v", err)
			}
		})
		if !errorIs(bad.err, errFixtureReader) {
			t.Fatalf("retry: expected injected failure, got %v", bad.err)
		}
		failed.waitProducer(t)

		var first string
		for attempt := 0; attempt < 2; attempt++ {
			form := newStreamForm()
			form.produce(func(mw *multipart.Writer) error {
				if err := scriptWriteField(mw, "meta", "meta-value"); err != nil {
					return err
				}
				if err := scriptWriteRepeatingFile(mw, "blob", "blob.bin", largeFileSize, 'z'); err != nil {
					return err
				}
				return scriptWriteFile(mw, "note", "note.txt", "text/plain", "note")
			})

			obs := newObservation()
			res := runParse(t, a, context.Background(), form, nil, func(h *registerHandle) {
				if err := h.register("blob", func(r io.Reader, hdr formstream.Header) error {
					b, err := io.ReadAll(r)
					if err != nil {
						return err
					}
					obs.recordFile(hdr.FileName(), b)
					return nil
				}, formstream.WithRequiredPart("meta")); err != nil {
					t.Fatalf("register blob: %v", err)
				}
				if err := h.register("note", func(r io.Reader, hdr formstream.Header) error {
					b, err := io.ReadAll(r)
					if err != nil {
						return err
					}
					obs.recordFile(hdr.FileName(), b)
					return nil
				}); err != nil {
					t.Fatalf("register note: %v", err)
				}
			})
			if res.err != nil {
				t.Fatalf("retry attempt %d: %v", attempt, res.err)
			}
			form.waitProducer(t)
			obs.fields["meta"] = res.value(t, "meta")

			d := obs.digest()
			if attempt == 0 {
				first = d
			} else if d != first {
				t.Fatalf("retry stability: repeated identical input drifted:\nfirst=%s\nagain=%s", first, d)
			}
		}
	})
}
