package conformance

import (
	"context"
	"errors"

	"github.com/mazrean/formstream"
)

// Snapshot is the observable post-parse parser state.
type Snapshot map[string]string

// ValuesSnapshot reads parser values through the public Value API.
func ValuesSnapshot(p *formstream.Parser, keys ...string) Snapshot {
	out := make(Snapshot, len(keys))
	for _, k := range keys {
		if v, _, ok := p.Value(k); ok {
			out[k] = v
		}
	}
	return out
}

// ErrorClass categorizes errors according to the existing error layering.
type ErrorClass string

const (
	ClassNone             ErrorClass = "none"
	ClassTooManyParts     ErrorClass = "too-many-parts"
	ClassTooManyHeaders   ErrorClass = "too-many-headers"
	ClassTooLargeForm     ErrorClass = "too-large-form"
	ClassMalformed        ErrorClass = "malformed-input"
	ClassConsumerFailure  ErrorClass = "consumer-failure"
	ClassReaderFailure    ErrorClass = "reader-failure"
	ClassContextCancelled ErrorClass = "context-cancelled"
)

// Classify maps an error to the public error class it belongs to.
func Classify(err error) ErrorClass {
	switch {
	case err == nil:
		return ClassNone
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ClassContextCancelled
	case errors.Is(err, formstream.ErrTooManyParts):
		return ClassTooManyParts
	case errors.Is(err, formstream.ErrTooManyHeaders):
		return ClassTooManyHeaders
	case errors.Is(err, formstream.ErrTooLargeForm):
		return ClassTooLargeForm
	case errors.Is(err, ErrConsumerFixture):
		return ClassConsumerFailure
	case errors.Is(err, ErrReaderFixture):
		return ClassReaderFailure
	default:
		return ClassMalformed
	}
}
