package actions

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
)

func TestIndexVersionAdvancedOnReplica(t *testing.T) {
	t.Parallel()

	const (
		ledger     = "ledger"
		preVersion = uint32(1)
	)

	id := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")
	canonical := indexes.Canonical(id)
	matches := func(got *ledgerpb.IndexID) bool {
		return indexes.Canonical(got) == canonical
	}

	tests := []struct {
		name    string
		current uint32
		pending uint32
		wantErr bool
	}{
		{
			name:    "old incarnation still looks ready",
			current: preVersion,
			wantErr: true,
		},
		{
			name:    "new incarnation is building",
			current: 0,
			pending: 2,
			wantErr: true,
		},
		{
			name:    "new incarnation switched",
			current: 2,
		},
		{
			name:    "later build is in flight",
			current: 2,
			pending: 3,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := &ledgerpb.GetIndexStatusResponse{
				Indexes: []*ledgerpb.IndexEntry{
					{
						Ledger:         ledger,
						Index:          &ledgerpb.Index{Id: id},
						CurrentVersion: tt.current,
						PendingVersion: tt.pending,
					},
				},
			}

			err := indexVersionAdvancedOnReplica(resp, ledger, matches, preVersion, "metadata[tier]")
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
