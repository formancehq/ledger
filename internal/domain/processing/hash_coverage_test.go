package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func testOrders() []*raftcmdpb.Order {
	return []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "test"}}},
		{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "test2"}}},
	}
}

func TestComputeAuditHash_BLAKE3(t *testing.T) {
	t.Parallel()

	orders := testOrders()

	_, hash1 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, orders)
	require.Len(t, hash1, 32, "BLAKE3 should produce 32-byte hash")

	// Chaining: different lastAuditHash produces different result
	_, hash2 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, []byte("prev-hash"), orders)
	require.NotEqual(t, hash1, hash2, "different lastAuditHash should produce different hash")
}

func TestComputeAuditHash_XXH3(t *testing.T) {
	t.Parallel()

	_, hash := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, nil, nil, testOrders()[:1])
	require.Len(t, hash, 16, "XXH3 should produce 16-byte hash")
}

func TestComputeAuditHash_EmptyOrders(t *testing.T) {
	t.Parallel()

	_, hash := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil)
	require.Len(t, hash, 32, "empty orders should still produce a hash")
}

func TestComputeAuditHashByVersion(t *testing.T) {
	t.Parallel()

	orders := testOrders()[:1]

	_, hashBLAKE3 := ComputeAuditHashByVersion(0, nil, nil, orders) // 0 = BLAKE3
	require.Len(t, hashBLAKE3, 32)

	_, hashXXH3 := ComputeAuditHashByVersion(1, nil, nil, orders) // 1 = XXH3
	require.Len(t, hashXXH3, 16)
}

func TestComputeAuditHash_Deterministic(t *testing.T) {
	t.Parallel()

	orders := testOrders()
	lastHash := []byte("chain")

	_, hash1 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, lastHash, orders)
	_, hash2 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, lastHash, orders)
	require.Equal(t, hash1, hash2, "same inputs must produce same hash")
}
