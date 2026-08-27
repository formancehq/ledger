// Package readdiag accumulates per-request serving diagnostics for read
// RPCs. The gRPC diag interceptors seed a collector into the request
// context and expose the result as the x-diag response trailer; the
// routing and storage layers annotate it as the read travels through
// them. The model workload copies the trailer into its finding details,
// so a violating read names the node, route, barrier state, and store
// fold points that produced it.
package readdiag

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Diag is a concurrency-safe key→value collector for one request.
type Diag struct {
	mu     sync.Mutex
	fields map[string]string
}

// New returns an empty collector.
func New() *Diag {
	return &Diag{fields: map[string]string{}}
}

// Set records one field. Nil-safe so callers can annotate unconditionally.
func (d *Diag) Set(key string, value any) {
	if d == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.fields[key] = fmt.Sprint(value)
}

// String renders the fields as a stable "k=v k=v" line.
func (d *Diag) String() string {
	if d == nil {
		return ""
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	keys := make([]string, 0, len(d.fields))
	for k := range d.fields {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+d.fields[k])
	}

	return strings.Join(parts, " ")
}

type diagKey struct{}

// WithDiag returns ctx carrying the collector.
func WithDiag(ctx context.Context, d *Diag) context.Context {
	return context.WithValue(ctx, diagKey{}, d)
}

// FromContext returns the request's collector, or nil when the request
// was not seeded by the diag interceptor.
func FromContext(ctx context.Context) *Diag {
	d, _ := ctx.Value(diagKey{}).(*Diag)

	return d
}

// Set annotates the context's collector; no-op without one.
func Set(ctx context.Context, key string, value any) {
	FromContext(ctx).Set(key, value)
}
