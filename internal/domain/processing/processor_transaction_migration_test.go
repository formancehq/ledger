package processing

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// Reproduces the volume imbalance caught in antithesis (events_volume_imbalance.log).
// While an account type is MIGRATING, resolveMigratingVolumes merges old-address
// volumes into the new address but (a) does not zero the old key and (b) aliases
// the new key to the same pointer on the "no existing newVol" branch. The aggregate
// over all volume keys therefore double-counts the migrated balance, producing an
// input/output imbalance equal to the migrated account's net position.
func TestResolveMigratingVolumes_DoubleCountsOldKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		ledger = "default"
		asset  = "COIN"
	)

	oldKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: ledger, Account: "users:1"},
		Asset:      asset,
	}
	newKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: ledger, Account: "newusers:1"},
		Asset:      asset,
	}

	// users:1 previously received 50 COIN from world (net-receiver).
	oldVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256(uint256.NewInt(50)),
		Output: commonpb.NewUint256(uint256.NewInt(0)),
	}

	mockStore := NewMockInMemoryStore(ctrl)
	mockStore.EXPECT().GetVolume(oldKey).Return(oldVol, nil)
	mockStore.EXPECT().GetVolume(newKey).Return(nil, domain.ErrNotFound)

	// Capture all PutVolume calls so we can inspect the post-resolve state.
	written := make(map[domain.VolumeKey]*raftcmdpb.VolumePair)
	mockStore.EXPECT().
		PutVolume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(k domain.VolumeKey, v *raftcmdpb.VolumePair) {
			written[k] = v
		}).
		AnyTimes()

	info := &commonpb.LedgerInfo{
		Name: ledger,
		AccountTypes: map[string]*commonpb.AccountType{
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
				Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING,
				Migration: &commonpb.AccountTypeMigration{
					TargetPattern: "newusers:{id}",
				},
			},
		},
	}

	order := &raftcmdpb.CreateTransactionOrder{
		Postings: []*commonpb.Posting{{
			Source:      "world",
			Destination: "newusers:1",
			Amount:      commonpb.NewUint256(uint256.NewInt(7)),
			Asset:       asset,
		}},
	}

	resolveMigratingVolumes(mockStore, ledger, order, info)

	// Bug 1: old key was never zeroed.
	if oldStored, ok := written[oldKey]; ok {
		var in, out uint256.Int
		oldStored.GetInput().IntoUint256(&in)
		oldStored.GetOutput().IntoUint256(&out)
		require.Truef(t, in.IsZero() && out.IsZero(),
			"BUG: old key was written but not zeroed: Input=%s Output=%s", in.Dec(), out.Dec())
	} else {
		t.Fatal("BUG: old key was never re-written with a zero volume — aggregate will double-count it")
	}

	// Bug 2: new key aliases the old-volume pointer.
	newStored, ok := written[newKey]
	require.True(t, ok, "new key should have been written")
	require.NotSamef(t, oldVol, newStored,
		"BUG: new key stores the same pointer as oldVol — applyPosting mutations will leak to the old key")
}
