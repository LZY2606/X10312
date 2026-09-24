package conformance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/mazrean/formstream"
)

// FileMode declares how a stream hook consumes a file part.
type FileMode string

const (
	// ModeFull drains the complete part.
	ModeFull FileMode = "full"
	// ModePrefix reads at most Prefix bytes and returns nil (consumer
	// stops early without an error).
	ModePrefix FileMode = "prefix"
	// ModeError reads at most Prefix bytes and then returns a non-nil
	// error (injected consumer failure).
	ModeError FileMode = "error"
	// ModeSlow drains the part in small chunks with delays, producing
	// back-pressure on the producer goroutine.
	ModeSlow FileMode = "slow"
)

// FileSpec configures the hook for one file field.
type FileSpec struct {
	Mode         FileMode
	Prefix       int64
	Requirements []string
}

// Consumer declares observable consumer behavior for an attempt.
type Consumer struct {
	Files     map[string]FileSpec
	SlowDelay time.Duration
}

// NewHook builds the single shared stream hook used by every adapter. It
// records observable calls and meters drained bytes. The hook is the real
// production hook function passed to Register; the harness supplies its
// body but execution stays on the production hook path.
func NewHook(meter *Meter, rec *Recorder, consumer Consumer) formstream.StreamHookFunc {
	if consumer.SlowDelay == 0 {
		consumer.SlowDelay = 200 * time.Microsecond
	}
	return func(r io.Reader, h formstream.Header) error {
		spec, ok := consumer.Files[h.Name()]
		if !ok {
			spec = FileSpec{Mode: ModeFull}
		}

		cr := &consumerReader{r: r, m: meter}
		var (
			n        int64
			sum      hash.Hash
			drainErr error
		)

		switch spec.Mode {
		case ModePrefix, ModeError:
			limit := spec.Prefix
			if limit == 0 {
				limit = 32
			}
			n, sum, drainErr = drain(io.LimitReader(cr, limit))
			rec.Record(h.Name(), h.FileName(), h.ContentType(), n, hex.EncodeToString(sum.Sum(nil)))
			if spec.Mode == ModeError {
				return fmt.Errorf("hook on %q: %w", h.Name(), ErrConsumerFixture)
			}
			return drainErr
		case ModeSlow:
			n, sum, drainErr = drainSlow(cr, consumer.SlowDelay)
		default:
			n, sum, drainErr = drain(cr)
		}
		rec.Record(h.Name(), h.FileName(), h.ContentType(), n, hex.EncodeToString(sum.Sum(nil)))
		return drainErr
	}
}

func drain(r io.Reader) (int64, hash.Hash, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	return n, h, err
}

func drainSlow(r io.Reader, delay time.Duration) (int64, hash.Hash, error) {
	h := sha256.New()
	buf := make([]byte, 256)
	var total int64
	for {
		time.Sleep(delay)
		n, err := r.Read(buf)
		_, _ = h.Write(buf[:n])
		total += int64(n)
		if err != nil {
			if err == io.EOF {
				return total, h, nil
			}
			return total, h, err
		}
	}
}
