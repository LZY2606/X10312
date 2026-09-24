package conformance

import (
	"errors"
	"hash/fnv"
	"io"
	"sync"
	"time"

	"github.com/mazrean/formstream"
)

// HookSpec is the consumer action for one registered part name and the
// observable results of every hook invocation. The hook runs on the real
// production hook path; this type only scripts what the caller does.
type HookSpec struct {
	Name     string
	Required []string
	// Consume >= 0 reads only that many prefix bytes; -1 drains all.
	Consume int64
	// ReturnErr is returned after the prefix was consumed.
	ReturnErr error
	// Throttle sleeps after every drained read, modeling a slow consumer.
	Throttle time.Duration
	// ExpectContent verifies every invocation received the exact file bytes.
	ExpectContent bool
	// ExpectedBytes, when >= 0, asserts the exact consumed length of each call.
	ExpectedBytes int64
	// Expected header values (checked on first invocation).
	ObservedFilename string
	ObservedType     string

	obs *observations
	// files are the ordered body files sharing this hook's part name.
	files []File
}

func Spec(name string, opts ...func(*HookSpec)) *HookSpec {
	h := &HookSpec{Name: name, Consume: -1, ExpectedBytes: -1, obs: &observations{}}
	for _, o := range opts {
		o(h)
	}
	return h
}

func WithRequired(parts ...string) func(*HookSpec) {
	return func(h *HookSpec) { h.Required = parts }
}

func ReadPrefix(n int64) func(*HookSpec) {
	return func(h *HookSpec) { h.Consume = n }
}

func ReturnError(err error) func(*HookSpec) {
	return func(h *HookSpec) { h.ReturnErr = err }
}

// ThrottleReads makes the consumer pause after each read (slow consumer).
func ThrottleReads(d time.Duration) func(*HookSpec) {
	return func(h *HookSpec) { h.Throttle = d }
}

func CheckContent() func(*HookSpec) {
	return func(h *HookSpec) { h.ExpectContent = true }
}

func ExpectBytes(n int64) func(*HookSpec) {
	return func(h *HookSpec) { h.ExpectedBytes = n }
}

func ExpectFilename(name string) func(*HookSpec) {
	return func(h *HookSpec) { h.ObservedFilename = name }
}

func ExpectContentType(ct string) func(*HookSpec) {
	return func(h *HookSpec) { h.ObservedType = ct }
}

type invokeObs struct {
	bytes  int64
	hash   uint64
	source fileKey
}

type fileKey struct {
	name  string
	index int
}

type observations struct {
	mu          sync.Mutex
	invokes     []invokeObs
	filename    string
	contentType string
}

// bind records the ordered files behind this hook name.
func (h *HookSpec) bind(b Body) {
	h.files = h.files[:0]
	for _, f := range b.Files {
		if f.Name == h.Name {
			h.files = append(h.files, f)
		}
	}
}

func expectedReadLength(h *HookSpec, f File) int64 {
	if h.Consume < 0 {
		return int64(f.Size)
	}
	return min(h.Consume, int64(f.Size))
}

func (o *observations) callCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.invokes)
}

func (o *observations) snapshot() []invokeObs {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]invokeObs, len(o.invokes))
	copy(out, o.invokes)
	return out
}

// register performs the public Register call. filesOf maps the part name to
// the ordered body files so content integrity can be checked per invocation.
func (h *HookSpec) register(reg Registrar, src *trackedReadCloser, filesOf []File) {
	opts := make([]formstream.RegisterOption, 0, len(h.Required))
	for _, r := range h.Required {
		opts = append(opts, formstream.WithRequiredPart(r))
	}
	err := reg(h.Name, func(r io.Reader, header formstream.Header) error {
		h.obs.mu.Lock()
		callIndex := len(h.obs.invokes)
		if h.obs.filename == "" {
			h.obs.filename = header.FileName()
		}
		if h.obs.contentType == "" {
			h.obs.contentType = header.ContentType()
		}
		h.obs.mu.Unlock()

		n, hash, err := h.consume(r, src)
		var src fileKey
		if callIndex < len(filesOf) {
			src = fileKey{name: filesOf[callIndex].Name, index: callIndex}
		}
		h.obs.mu.Lock()
		h.obs.invokes = append(h.obs.invokes, invokeObs{bytes: n, hash: hash, source: src})
		h.obs.mu.Unlock()
		return err
	}, opts...)
	if err != nil {
		panic(err)
	}
}

// consume executes the scripted consumer behavior and folds a checksum over
// the bytes actually delivered, without retaining them.
func (h *HookSpec) consume(r io.Reader, src *trackedReadCloser) (int64, uint64, error) {
	hasher := fnv.New64a()
	var total int64

	var reader io.Reader = r
	if h.Consume >= 0 {
		reader = io.LimitReader(r, h.Consume)
	}

	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			total += int64(n)
			_, _ = hasher.Write(buf[:n])
			src.Consumed(int64(n))
		}
		if h.Throttle > 0 {
			time.Sleep(h.Throttle)
		}
		if err != nil {
			if err == io.EOF {
				return total, hasher.Sum64(), h.ReturnErr
			}
			// Preserve the real read failure (reader died, unexpected EOF,
			// context-caused close): a consumer hook returning it is how the
			// causal error reaches the parser on the fast path.
			return total, hasher.Sum64(), errorsJoin(err, h.ReturnErr)
		}
	}
}

func expectedHash(f File, length int64) uint64 {
	h := fnv.New64a()
	buf := make([]byte, 4096)
	var off int64
	for off < length {
		end := min(off+int64(len(buf)), length)
		for i := off; i < end; i++ {
			fn := f.Content
			if fn == nil {
				fn = deterministic
			}
			buf[i-off] = fn(int(i))
		}
		_, _ = h.Write(buf[:end-off])
		off = end
	}
	return h.Sum64()
}

// errorsJoin joins a read failure with an optional consumer error; the read
// failure keeps first position as it is the originating cause.
func errorsJoin(readErr, consumerErr error) error {
	if consumerErr == nil {
		return readErr
	}
	return errors.Join(readErr, consumerErr)
}
