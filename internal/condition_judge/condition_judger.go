package conditionjudge

import (
	"errors"
	"fmt"
)

type IConditionJudger[K comparable, S any, T any] interface {
	IsHookExist(key K) bool
	HookEvent(key K, value S) (bool, error)
	KeyEvent(key K) error
}

// ConditionJudger If the condition is met, execute immediately; if not, wait until it is met and then execute.
type ConditionJudger[K comparable, S any, T any] struct {
	// preProcessFunc Convert value conversions when conditions are not met
	preProcessFunc     PreProcessFunc[S, T]
	satisfiedHookMap   map[K]func(S) error
	unsatisfiedHookMap map[K]*waitHook[K, S, T]
	requirementHookMap map[K][]*waitHook[K, S, T]
	closeFunc          func(T) error
}

type PreProcessFunc[S any, T any] func(S) (T, error)

// Option customizes a ConditionJudger.
type Option[K comparable, S any, T any] func(*ConditionJudger[K, S, T])

// WithCloseFunc registers a function invoked for every pre-processed value
// that never reached a hook when Close is called. It guarantees resources
// produced by PreProcessFunc are released even when parsing aborts early.
func WithCloseFunc[K comparable, S any, T any](fn func(T) error) Option[K, S, T] {
	return func(w *ConditionJudger[K, S, T]) {
		w.closeFunc = fn
	}
}

type waitHook[K comparable, S any, T any] struct {
	key              K
	normalPathFunc   func(S) error
	abnormalPathFunc func(T) error
	callParams       []T
	unsatisfiedCount int
}

type Hook[K comparable, S any, T any] interface {
	NormalPath(S) error
	AbnormalPath(T) error
	Requirements() []K
}

func NewConditionJudger[K comparable, S any, T any](hookMap map[K]Hook[K, S, T], prepreProcessFunc PreProcessFunc[S, T], opts ...Option[K, S, T]) *ConditionJudger[K, S, T] {
	satisfiedHookMap := make(map[K]func(S) error, len(hookMap))
	unsatisfiedHookMap := make(map[K]*waitHook[K, S, T])
	requirementHookMap := make(map[K][]*waitHook[K, S, T])
	for key, hook := range hookMap {
		requirements := hook.Requirements()

		if len(requirements) == 0 {
			satisfiedHookMap[key] = hook.NormalPath
			continue
		}

		hookValue := &waitHook[K, S, T]{
			key:              key,
			normalPathFunc:   hook.NormalPath,
			abnormalPathFunc: hook.AbnormalPath,
			unsatisfiedCount: len(requirements),
		}
		unsatisfiedHookMap[key] = hookValue
		for _, requirePart := range requirements {
			requirementHookMap[requirePart] = append(requirementHookMap[requirePart], hookValue)
		}
	}

	w := &ConditionJudger[K, S, T]{
		preProcessFunc:     prepreProcessFunc,
		satisfiedHookMap:   satisfiedHookMap,
		unsatisfiedHookMap: unsatisfiedHookMap,
		requirementHookMap: requirementHookMap,
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Close releases pre-processed values still buffered for hooks whose
// requirements were never satisfied.
func (w *ConditionJudger[K, S, T]) Close() error {
	if w.closeFunc == nil {
		return nil
	}

	var errs []error
	for _, hook := range w.unsatisfiedHookMap {
		for _, param := range hook.callParams {
			if err := w.closeFunc(param); err != nil {
				errs = append(errs, err)
			}
		}
		hook.callParams = nil
	}

	return errors.Join(errs...)
}

func (w *ConditionJudger[K, S, T]) IsHookExist(key K) bool {
	if _, ok := w.satisfiedHookMap[key]; ok {
		return true
	}

	_, ok := w.unsatisfiedHookMap[key]
	return ok
}

var ErrNoHooks = errors.New("no hooks")

func (w *ConditionJudger[K, S, T]) HookEvent(key K, value S) (bool, error) {
	if fn := w.satisfiedHookMap[key]; fn != nil {
		err := fn(value)
		if err != nil {
			return false, fmt.Errorf("failed to execute hook: %w", err)
		}

		return true, nil
	}

	callParam, err := w.preProcessFunc(value)
	if err != nil {
		return false, fmt.Errorf("failed to pre-process: %w", err)
	}

	hookValue, ok := w.unsatisfiedHookMap[key]
	if !ok {
		return false, ErrNoHooks
	}
	hookValue.callParams = append(hookValue.callParams, callParam)

	return false, nil
}

func (w *ConditionJudger[K, S, T]) KeyEvent(key K) error {
	hooks := w.requirementHookMap[key]

	var errs []error
	for _, hook := range hooks {
		if hook.unsatisfiedCount <= 0 {
			continue
		}
		hook.unsatisfiedCount--
		if hook.unsatisfiedCount > 0 {
			continue
		}

		delete(w.unsatisfiedHookMap, hook.key)
		w.satisfiedHookMap[hook.key] = hook.normalPathFunc

		for _, param := range hook.callParams {
			err := hook.abnormalPathFunc(param)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to execute hook(%v): %w", key, err))
			}
		}
		hook.callParams = nil
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}

	return nil
}
