package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaitForQuiescentCommitIndexRejectsEmptyBudget(t *testing.T) {
	t.Parallel()

	index, err := WaitForQuiescentCommitIndex(context.Background(), nil, 0)
	require.Zero(t, index)
	require.ErrorContains(t, err, "must be positive")
}
