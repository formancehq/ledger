package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestGenerateRandomID(t *testing.T) {
	t.Parallel()

	// Generate multiple IDs and verify they're unique
	seen := make(map[uint64]struct{}, 1000)
	for range 1000 {
		id := GenerateRandomID()
		require.NotContains(t, seen, id, "duplicate ID generated: %d", id)
		seen[id] = struct{}{}
	}
}

func TestNewCommand(t *testing.T) {
	t.Parallel()

	order := &raftcmdpb.Order{}
	cmd := NewCommand(order)

	require.NotZero(t, cmd.GetId())
	require.Len(t, cmd.GetOrders(), 1)
	require.NotNil(t, cmd.GetDate())
	require.NotNil(t, cmd.GetPreload())
}

func TestNewCommandMultipleOrders(t *testing.T) {
	t.Parallel()

	orders := []*raftcmdpb.Order{{}, {}, {}}
	cmd := NewCommand(orders...)

	require.Len(t, cmd.GetOrders(), 3)
}

func TestNewCommandNoOrders(t *testing.T) {
	t.Parallel()

	cmd := NewCommand()

	require.NotZero(t, cmd.GetId())
	require.Empty(t, cmd.GetOrders())
	require.NotNil(t, cmd.GetDate())
	require.NotNil(t, cmd.GetPreload())
}
