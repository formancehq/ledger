package internal

import (
	"math/big"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestPITIdempotencyKeyUsesConfiguredLimitFamily(t *testing.T) {
	t.Parallel()

	for _, length := range PITIdempotencyKeyLengths() {
		key, err := PITIdempotencyKey(1, 2, length)
		require.NoError(t, err)
		require.Len(t, key, length)
	}

	_, err := PITIdempotencyKey(1, 2, PITIdempotencyMaximumKeyLength+1)
	require.Error(t, err)
}

func TestPITIdempotencyProbeReachedRequiresExactHeader(t *testing.T) {
	t.Parallel()

	require.True(t, PITIdempotencyProbeReached(metadata.Pairs(
		PITIdempotencyProbeReachedMetadataKey,
		"probe-1",
	), "probe-1"))
	require.False(t, PITIdempotencyProbeReached(metadata.Pairs(
		PITIdempotencyProbeReachedMetadataKey,
		"other",
	), "probe-1"))
}

func TestValidatePITKeyedAudit(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence:    7,
		Idempotency: &commonpb.Idempotency{Key: "key"},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: 11,
			MaxLogSequence: 11,
		}},
		OrderCount: 1,
		Items: []*auditpb.AuditItem{{
			OrderIndex:  0,
			LogSequence: 11,
		}},
	}

	auditSequence, err := ValidatePITKeyedAudit([]*auditpb.AuditEntry{entry}, "key", 11)
	require.NoError(t, err)
	require.Equal(t, uint64(7), auditSequence)

	_, err = ValidatePITKeyedAudit([]*auditpb.AuditEntry{entry, entry}, "key", 11)
	require.Error(t, err)
}

func TestValidatePITSingleInput(t *testing.T) {
	t.Parallel()

	result := &commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{{
		Asset:  "USD/2",
		Input:  commonpb.NewUint256FromUint64(42),
		Output: commonpb.NewUint256FromUint64(0),
	}}}

	require.NoError(t, ValidatePITSingleInput(result, "USD/2", big.NewInt(42)))
	require.Error(t, ValidatePITSingleInput(result, "USD/2", big.NewInt(41)))
}
