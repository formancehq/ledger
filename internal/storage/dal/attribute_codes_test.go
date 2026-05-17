package dal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// TestAttributeCodesUnique ensures no two attribute codes share the same byte value.
// This prevents silent key collisions if a new code is added carelessly.
func TestAttributeCodesUnique(t *testing.T) {
	t.Parallel()

	codes := map[byte]string{
		dal.AttributeCodeVolume:           "Volume",
		dal.AttributeCodeMetadata:         "Metadata",
		dal.AttributeCodeIdempotency:      "Idempotency",
		dal.AttributeCodeReference:        "Reference",
		dal.AttributeCodeLedger:           "Ledger",
		dal.AttributeCodeBoundary:         "Boundary",
		dal.AttributeCodeTransaction:      "Transaction",
		dal.AttributeCodeSinkConfig:       "SinkConfig",
		dal.AttributeCodeNumscriptVersion: "NumscriptVersion",
		dal.AttributeCodeNumscriptContent: "NumscriptContent",
		dal.AttributeCodePreparedQuery:    "PreparedQuery",
		dal.AttributeCodeLedgerMetadata:   "LedgerMetadata",
	}

	// If two codes mapped to the same byte, the map would silently keep only the last one.
	// Verify by checking the expected count.
	require.Len(t, codes, 12, "duplicate attribute code detected — map collapsed two entries with the same byte value")
}

// TestKeyPrefixesUnique ensures no two key prefix constants share the same byte value.
func TestKeyPrefixesUnique(t *testing.T) {
	t.Parallel()

	prefixes := map[byte]string{
		dal.KeyPrefixLog:                      "Log",
		dal.KeyPrefixAudit:                    "Audit",
		dal.KeyPrefixIdempotency:              "Idempotency",
		dal.KeyPrefixIdempotencyTimeIdx:       "IdempotencyTimeIdx",
		dal.KeyPrefixPreparedQuery:            "PreparedQuery",
		dal.KeyPrefixPendingLedgerCleanup:     "PendingLedgerCleanup",
		dal.KeyPrefixQueryCheckpoint:          "QueryCheckpoint",
		dal.KeyPrefixNextQueryCheckpointID:    "NextQueryCheckpointID",
		dal.KeyPrefixQueryCheckpointSchedule:  "QueryCheckpointSchedule",
		dal.KeyPrefixReversions:               "Reversions",
		dal.KeyPrefixClusterConfig:            "ClusterConfig",
		dal.KeyPrefixBloom:                    "Bloom",
		dal.KeyPrefixMirrorSourceHead:         "MirrorSourceHead",
		dal.KeyPrefixMirrorCursor:             "MirrorCursor",
		dal.KeyPrefixMirrorStatus:             "MirrorStatus",
		dal.KeyPrefixPeriodSchedule:           "PeriodSchedule",
		dal.KeyPrefixAttributes:               "Attributes",
		dal.KeyPrefixLastAppliedIndex:         "LastAppliedIndex",
		dal.KeyPrefixLastAppliedTimestamp:      "LastAppliedTimestamp",
		dal.KeyPrefixLedgerInfo:               "LedgerInfo",
		dal.KeyPrefixSigningKey:               "SigningKey",
		dal.KeyPrefixPeriods:                  "Periods",
		dal.KeyPrefixNextPeriodID:             "NextPeriodID",
		dal.KeyPrefixSigningConfig:            "SigningConfig",
		dal.KeyPrefixSinkCursor:               "SinkCursor",
		dal.KeyPrefixEventsConfig:             "EventsConfig",
		dal.KeyPrefixSinkStatus:               "SinkStatus",
		dal.KeyPrefixMaintenanceMode:          "MaintenanceMode",
		dal.KeyPrefixPersistedConfig:          "PersistedConfig",
		dal.KeyPrefixCacheSnapshot:            "CacheSnapshot",
	}

	require.Len(t, prefixes, 30, "duplicate key prefix detected — map collapsed two entries with the same byte value")
}
