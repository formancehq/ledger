package query

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

type horizonCloser struct {
	closed bool
}

func (c *horizonCloser) Close() error {
	c.closed = true

	return nil
}

func TestMainHorizonKeep_Logs(t *testing.T) {
	t.Parallel()

	const (
		ledgerName = "test"
		logID      = uint64(17)
		mainSeq    = uint64(42)
	)

	tests := []struct {
		name            string
		entity          []byte
		indexValue      []byte
		getErr          error
		wantKeep        bool
		wantErrContains string
		wantErrIs       error
		expectGet       bool
		expectClose     bool
	}{
		{
			name:            "malformed log entity length",
			entity:          []byte{0x01},
			wantErrContains: "log entity of unexpected length 1 (want 8)",
		},
		{
			name:            "index get failure",
			entity:          logID8(logID),
			getErr:          errors.New("index unavailable"),
			wantErrContains: "resolving log id: index unavailable",
			expectGet:       true,
		},
		{
			name:            "index entry not found",
			entity:          logID8(logID),
			getErr:          pebble.ErrNotFound,
			wantErrContains: "resolving log id",
			wantErrIs:       pebble.ErrNotFound,
			expectGet:       true,
		},
		{
			name:            "malformed index value length",
			entity:          logID8(logID),
			indexValue:      []byte{0x01},
			wantErrContains: "log index value of unexpected length 1 (want 8)",
			expectGet:       true,
			expectClose:     true,
		},
		{
			name:        "sequence equal to main horizon",
			entity:      logID8(logID),
			indexValue:  ledgerLogIndexValue(mainSeq),
			wantKeep:    true,
			expectGet:   true,
			expectClose: true,
		},
		{
			name:        "sequence above main horizon",
			entity:      logID8(logID),
			indexValue:  ledgerLogIndexValue(mainSeq + 1),
			wantKeep:    false,
			expectGet:   true,
			expectClose: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			getter := NewMockPebbleGetter(gomock.NewController(t))
			closer := &horizonCloser{}
			if tt.expectGet {
				kb := dal.NewKeyBuilder()
				getCall := getter.EXPECT().
					Get(readstore.LedgerLogKey(kb, ledgerName, logID)).
					Return(tt.indexValue, closer, nil)
				if tt.getErr != nil {
					getCall.Return(nil, nil, tt.getErr)
				}
			}

			keep := MainHorizonKeep(
				commonpb.QueryTarget_QUERY_TARGET_LOGS,
				nil,
				getter,
				ledgerName,
				mainSeq,
			)
			require.NotNil(t, keep)

			gotKeep, err := keep(tt.entity)
			if tt.wantErrContains != "" {
				require.ErrorContains(t, err, tt.wantErrContains)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantKeep, gotKeep)
			}
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
			}
			require.Equal(t, tt.expectClose, closer.closed)
		})
	}
}
