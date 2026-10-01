package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithConsistency(t *testing.T) {
	t.Parallel()

	ctx := WithConsistency(context.Background(), ConsistencyStale)
	require.Equal(t, ConsistencyStale, ConsistencyFromContext(ctx))
}

func TestConsistencyFromContext_Default(t *testing.T) {
	t.Parallel()

	require.Equal(t, ConsistencyLinearizable, ConsistencyFromContext(context.Background()))
}

func TestConsistencyFromContext_EmptyString(t *testing.T) {
	t.Parallel()

	ctx := WithConsistency(context.Background(), "")
	require.Equal(t, ConsistencyLinearizable, ConsistencyFromContext(ctx))
}

func TestParseConsistency(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		raw   string
		level string
		ok    bool
	}{
		{raw: "", level: ConsistencyLinearizable, ok: true},
		{raw: "   ", level: ConsistencyLinearizable, ok: true},
		{raw: "linearizable", level: ConsistencyLinearizable, ok: true},
		{raw: "stale", level: ConsistencyStale, ok: true},
		{raw: "  STALE ", level: ConsistencyStale, ok: true},
		{raw: "Linearizable", level: ConsistencyLinearizable, ok: true},
		// EN-1946 removed the `leader` selector; it must not come back as a
		// silently accepted alias.
		{raw: "leader", ok: false},
		{raw: "invalid-value", ok: false},
	} {
		level, ok := ParseConsistency(tc.raw)
		require.Equal(t, tc.ok, ok, "raw=%q", tc.raw)
		require.Equal(t, tc.level, level, "raw=%q", tc.raw)
	}
}
