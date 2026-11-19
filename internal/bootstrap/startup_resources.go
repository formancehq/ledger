package bootstrap

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

// StartupResources owns resources constructed before Fx starts. Fx runs
// OnStop hooks after a successful Start (and during OnStart rollback), but a
// provider error prevents those hooks from running. CloseAll covers that gap.
type StartupResources struct {
	mu      sync.Mutex
	entries []*startupResource
	byKey   map[any]*startupResource
}

type startupResource struct {
	once  sync.Once
	close func() error
	err   error
}

func (r *startupResource) Close() error {
	r.once.Do(func() { r.err = r.close() })

	return r.err
}

func NewStartupResources() *StartupResources {
	return &StartupResources{byKey: make(map[any]*startupResource)}
}

// Track registers a resource immediately after its constructor succeeds.
// key must be the resource's comparable pointer, shared with its OnStop hook.
func (r *StartupResources) Track(key any, closeFn func() error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byKey[key]; exists {
		panic(fmt.Sprintf("startup resource registered twice: %T", key))
	}
	entry := &startupResource{close: closeFn}
	r.entries = append(r.entries, entry)
	r.byKey[key] = entry
}

// Close is used by normal Fx OnStop hooks. CloseAll can call it again safely
// if a startup hook failed after Fx had already begun its rollback.
func (r *StartupResources) Close(key any) error {
	r.mu.Lock()
	entry := r.byKey[key]
	r.mu.Unlock()
	if entry == nil {
		return fmt.Errorf("startup resource was not registered: %T", key)
	}

	return entry.Close()
}

// CloseAll releases partially constructed resources in reverse open order.
func (r *StartupResources) CloseAll() error {
	r.mu.Lock()
	entries := append([]*startupResource(nil), r.entries...)
	r.mu.Unlock()
	var errs []error
	for _, entrie := range slices.Backward(entries) {
		errs = append(errs, entrie.Close())
	}

	return errors.Join(errs...)
}
