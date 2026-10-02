package query

import (
	"context"
	"strings"
)

// Consistency levels for read operations. The level travels in the request
// context so every transport (gRPC metadata, HTTP header) selects the route
// the same way: RoutedController.readCtrl is the only reader.
const (
	// ConsistencyLinearizable is the default: ReadIndex barrier on the local node.
	ConsistencyLinearizable = "linearizable"
	// ConsistencyStale skips the ReadIndex barrier and reads from the local store directly.
	// Data may lag behind the latest committed index.
	ConsistencyStale = "stale"
)

type consistencyKey struct{}

// WithConsistency returns a copy of ctx with the given consistency level stored.
func WithConsistency(ctx context.Context, level string) context.Context {
	return context.WithValue(ctx, consistencyKey{}, level)
}

// ConsistencyFromContext returns the consistency level from ctx, defaulting to linearizable.
func ConsistencyFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(consistencyKey{}).(string); ok && v != "" {
		return v
	}

	return ConsistencyLinearizable
}

// ParseConsistency normalizes a caller-supplied consistency selector (case and
// surrounding whitespace are ignored). An empty value selects the default. ok
// is false for any value that names no known level; each transport decides
// what an unknown value means.
func ParseConsistency(raw string) (level string, ok bool) {
	switch level = strings.ToLower(strings.TrimSpace(raw)); level {
	case "":
		return ConsistencyLinearizable, true
	case ConsistencyLinearizable, ConsistencyStale:
		return level, true
	default:
		return "", false
	}
}
