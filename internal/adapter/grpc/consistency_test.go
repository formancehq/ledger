package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/query"
)

func TestExtractConsistency_NoMetadata(t *testing.T) {
	t.Parallel()

	// Context with no incoming metadata at all
	ctx := extractConsistency(context.Background())
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyLinearizable, level)
}

func TestExtractConsistency_MissingHeader(t *testing.T) {
	t.Parallel()

	// Incoming metadata present but without the consistency key
	md := metadata.New(map[string]string{"x-other": "value"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyLinearizable, level)
}

func TestExtractConsistency_InvalidValue(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "invalid-value"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyLinearizable, level)
}

func TestExtractConsistency_Stale(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "stale"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyStale, level)
}

func TestExtractConsistency_LeaderIsNotSupported(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "leader"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyLinearizable, level)
}

func TestExtractConsistency_CaseInsensitive(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "STALE"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyStale, level)
}

func TestExtractConsistency_Linearizable(t *testing.T) {
	t.Parallel()

	// "linearizable" is the default, so extractConsistency should not set it explicitly
	md := metadata.New(map[string]string{metadataKeyConsistency: "linearizable"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyLinearizable, level)
}

func TestExtractConsistency_WhitespaceHandling(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "  stale  "})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := query.ConsistencyFromContext(ctx)
	require.Equal(t, query.ConsistencyStale, level)
}
