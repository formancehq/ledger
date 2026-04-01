package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestWithConsistency(t *testing.T) {
	t.Parallel()

	ctx := WithConsistency(context.Background(), ConsistencyStale)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyStale, level)
}

func TestWithConsistency_Leader(t *testing.T) {
	t.Parallel()

	ctx := WithConsistency(context.Background(), ConsistencyLeader)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLeader, level)
}

func TestConsistencyFromContext_Default(t *testing.T) {
	t.Parallel()

	level := ConsistencyFromContext(context.Background())
	require.Equal(t, ConsistencyLinearizable, level)
}

func TestConsistencyFromContext_EmptyString(t *testing.T) {
	t.Parallel()

	ctx := WithConsistency(context.Background(), "")
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLinearizable, level)
}

func TestExtractConsistency_NoMetadata(t *testing.T) {
	t.Parallel()

	// Context with no incoming metadata at all
	ctx := extractConsistency(context.Background())
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLinearizable, level)
}

func TestExtractConsistency_MissingHeader(t *testing.T) {
	t.Parallel()

	// Incoming metadata present but without the consistency key
	md := metadata.New(map[string]string{"x-other": "value"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLinearizable, level)
}

func TestExtractConsistency_InvalidValue(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "invalid-value"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLinearizable, level)
}

func TestExtractConsistency_Stale(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "stale"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyStale, level)
}

func TestExtractConsistency_Leader(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "leader"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLeader, level)
}

func TestExtractConsistency_CaseInsensitive(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "STALE"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyStale, level)
}

func TestExtractConsistency_Linearizable(t *testing.T) {
	t.Parallel()

	// "linearizable" is the default, so extractConsistency should not set it explicitly
	md := metadata.New(map[string]string{metadataKeyConsistency: "linearizable"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLinearizable, level)
}

func TestExtractConsistency_WhitespaceHandling(t *testing.T) {
	t.Parallel()

	md := metadata.New(map[string]string{metadataKeyConsistency: "  leader  "})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx = extractConsistency(ctx)
	level := ConsistencyFromContext(ctx)
	require.Equal(t, ConsistencyLeader, level)
}
