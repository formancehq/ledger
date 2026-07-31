package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestKindForPointInTimeReasons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reason commonpb.ErrorReason
		number int32
		kind   ErrorKind
	}{
		{
			name:   "building",
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_BUILDING,
			number: 70,
			kind:   KindUnavailable,
		},
		{
			name:   "behind",
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_BEHIND,
			number: 71,
			kind:   KindUnavailable,
		},
		{
			name:   "expired",
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_EXPIRED,
			number: 72,
			kind:   KindPrecondition,
		},
		{
			name:   "source missing",
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_SOURCE_MISSING,
			number: 73,
			kind:   KindInternal,
		},
		{
			name:   "corrupt",
			reason: commonpb.ErrorReason_ERROR_REASON_HISTORY_CORRUPT,
			number: 74,
			kind:   KindInternal,
		},
		{
			name:   "unsupported temporal filter",
			reason: commonpb.ErrorReason_ERROR_REASON_UNSUPPORTED_TEMPORAL_FILTER,
			number: 75,
			kind:   KindValidation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.number, int32(test.reason))
			require.Equal(t, test.kind, KindForReason(test.reason))
		})
	}
}
