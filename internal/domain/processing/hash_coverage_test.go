package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func testLogs() []*commonpb.Log {
	return []*commonpb.Log{
		{Sequence: 1, Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{Name: "test"},
			},
		}},
		{Sequence: 2, Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{Name: "test2"},
			},
		}},
	}
}

func TestComputeAuditHash_BLAKE3(t *testing.T) {
	t.Parallel()

	logs := testLogs()

	_, hash1 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, logs)
	require.Len(t, hash1, 32, "BLAKE3 should produce 32-byte hash")

	// Chaining: different lastAuditHash produces different result
	_, hash2 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, []byte("prev-hash"), logs)
	require.NotEqual(t, hash1, hash2, "different lastAuditHash should produce different hash")
}

func TestComputeAuditHash_XXH3(t *testing.T) {
	t.Parallel()

	_, hash := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, nil, nil, nil, testLogs()[:1])
	require.Len(t, hash, 16, "XXH3 should produce 16-byte hash")
}

func TestComputeAuditHash_EmptyLogs(t *testing.T) {
	t.Parallel()

	_, hash := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, nil)
	require.Len(t, hash, 32, "empty logs should still produce a hash")
}

func TestComputeAuditHashForFailure(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence:   1,
		ProposalId: 42,
		OrderCount: 1,
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				ErrorType: "INSUFFICIENT_FUNDS",
				Message:   "not enough balance",
			},
		},
	}

	_, hash1 := ComputeAuditHashForFailure(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, entry)
	require.Len(t, hash1, 32)

	// Chaining
	_, hash2 := ComputeAuditHashForFailure(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, []byte("prev"), entry)
	require.NotEqual(t, hash1, hash2)
}

func TestComputeAuditHashByVersion(t *testing.T) {
	t.Parallel()

	logs := testLogs()[:1]

	_, hashBLAKE3 := ComputeAuditHashByVersion(0, nil, nil, logs) // 0 = BLAKE3
	require.Len(t, hashBLAKE3, 32)

	_, hashXXH3 := ComputeAuditHashByVersion(1, nil, nil, logs) // 1 = XXH3
	require.Len(t, hashXXH3, 16)
}

func TestComputeAuditHash_Deterministic(t *testing.T) {
	t.Parallel()

	logs := testLogs()
	lastHash := []byte("chain")

	_, hash1 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, lastHash, logs)
	_, hash2 := ComputeAuditHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, lastHash, logs)
	require.Equal(t, hash1, hash2, "same inputs must produce same hash")
}
