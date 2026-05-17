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
		dal.SubAttrVolume:           "Volume",
		dal.SubAttrMetadata:         "Metadata",
		dal.SubAttrReference:        "Reference",
		dal.SubAttrLedger:           "Ledger",
		dal.SubAttrBoundary:         "Boundary",
		dal.SubAttrTransaction:      "Transaction",
		dal.SubAttrSinkConfig:       "SinkConfig",
		dal.SubAttrNumscriptVersion: "NumscriptVersion",
		dal.SubAttrNumscriptContent: "NumscriptContent",
		dal.SubAttrPreparedQuery:    "PreparedQuery",
		dal.SubAttrLedgerMetadata:   "LedgerMetadata",
	}

	// If two codes mapped to the same byte, the map would silently keep only the last one.
	// Verify by checking the expected count.
	require.Len(t, codes, 11, "duplicate attribute code detected — map collapsed two entries with the same byte value")
}

// TestZonePrefixesUnique ensures no two zone prefix constants share the same byte value.
func TestZonePrefixesUnique(t *testing.T) {
	t.Parallel()

	zones := map[byte]string{
		dal.ZoneAttributes:  "Attributes",
		dal.ZoneCache:       "Cache",
		dal.ZonePerLedger:   "PerLedger",
		dal.ZoneCold:        "Cold",
		dal.ZoneIdempotency: "Idempotency",
		dal.ZoneGlobal:      "Global",
	}

	require.Len(t, zones, 6, "duplicate zone prefix detected — map collapsed two entries with the same byte value")
}
