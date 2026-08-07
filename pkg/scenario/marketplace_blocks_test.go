package scenario

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarketplaceBlocksRespectBalanceDependencies(t *testing.T) {
	t.Parallel()

	blocks := MarketplaceBlocks().Blocks
	names := make([]string, len(blocks))
	for i, block := range blocks {
		names[i] = block.Name
	}

	require.Equal(t, []string{
		"marketplace/deposit",
		"marketplace/purchase",
		"marketplace/payout",
		"marketplace/revert",
		"marketplace/metadata",
	}, names, "payout must run after purchase and before a random revert can remove its balance")
}

func TestMarketplaceCandidateScanCoversWholeFleet(t *testing.T) {
	t.Parallel()

	const count = 10
	seen := make(map[int]bool, count)
	start := 7
	for offset := range count {
		seen[marketplaceCandidateID(start, offset, count)] = true
	}

	for id := 1; id <= count; id++ {
		require.True(t, seen[id], "candidate scan missed account %d", id)
	}
}
