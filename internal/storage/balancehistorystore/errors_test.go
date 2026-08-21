package balancehistorystore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHistoricalBalanceErrorsAreDescribable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      domain.Describable
		reason   commonpb.ErrorReason
		kind     domain.ErrorKind
		metadata map[string]string
	}{
		{
			name:     "building",
			err:      &ErrBuilding{Current: 41, Target: 43},
			reason:   commonpb.ErrorReason_ERROR_REASON_HISTORY_BUILDING,
			kind:     domain.KindUnavailable,
			metadata: map[string]string{"currentLogSequence": "41", "targetLogSequence": "43"},
		},
		{
			name:     "behind",
			err:      &ErrBehind{Required: 43, Current: 41},
			reason:   commonpb.ErrorReason_ERROR_REASON_HISTORY_BEHIND,
			kind:     domain.KindUnavailable,
			metadata: map[string]string{"currentLogSequence": "41", "requiredLogSequence": "43"},
		},
		{
			name:   "source missing",
			err:    &ErrSourceMissing{Detail: "missing audit entry 42"},
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_SOURCE_MISSING,
			kind:   domain.KindInternal,
		},
		{
			name:   "corrupt",
			err:    &ErrCorrupt{Detail: "malformed segment"},
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_CORRUPT,
			kind:   domain.KindInternal,
		},
		{
			name:   "quarantined",
			err:    &ErrQuarantined{Detail: "invalid manifest"},
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_CORRUPT,
			kind:   domain.KindInternal,
		},
		{
			name:   "unsupported store format",
			err:    &ErrUnsupportedFormat{Found: 2, Supported: 1},
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_CORRUPT,
			kind:   domain.KindInternal,
		},
		{
			name:   "unsupported reducer format",
			err:    &ErrUnsupportedReducer{Found: 2, Supported: 1},
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_CORRUPT,
			kind:   domain.KindInternal,
		},
		{
			name:     "unsupported temporal filter",
			err:      &ErrUnsupportedTemporalFilter{Category: "historical-metadata"},
			reason:   commonpb.ErrorReason_ERROR_REASON_UNSUPPORTED_TEMPORAL_FILTER,
			kind:     domain.KindValidation,
			metadata: map[string]string{"filterCategory": "historical-metadata"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, domain.ReasonString(test.reason), test.err.Reason())
			require.Equal(t, test.reason, domain.ReasonCode(test.err.Reason()))
			require.Equal(t, test.kind, domain.Kind(test.err))
			require.Equal(t, test.metadata, test.err.Metadata())
		})
	}
}

func TestInternalHistoricalBalanceErrorsDoNotExposeDiagnosticMetadata(t *testing.T) {
	t.Parallel()

	require.Nil(t, (&ErrSourceMissing{Detail: "path=/secret"}).Metadata())
	require.Nil(t, (&ErrCorrupt{Detail: "run=18446744073709551615"}).Metadata())
	require.Nil(t, (&ErrQuarantined{Detail: "path=/secret"}).Metadata())
	require.Nil(t, (&ErrUnsupportedFormat{Found: 2, Supported: 1}).Metadata())
	require.Nil(t, (&ErrUnsupportedReducer{Found: 2, Supported: 1}).Metadata())
}

func TestUnsupportedTemporalFilterWithoutCategoryHasNoMetadata(t *testing.T) {
	t.Parallel()

	err := &ErrUnsupportedTemporalFilter{}
	require.Nil(t, err.Metadata())
	require.Equal(t, "filter is not supported for historical balance queries", err.Error())
}
