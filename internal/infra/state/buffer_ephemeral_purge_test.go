package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestIsVolumeZeroBalance(t *testing.T) {
	t.Parallel()

	t.Run("both nil", func(t *testing.T) {
		t.Parallel()
		require.True(t, isVolumeZeroBalance(&raftcmdpb.VolumePair{}))
	})

	t.Run("equal values", func(t *testing.T) {
		t.Parallel()
		v := &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(100),
		}
		require.True(t, isVolumeZeroBalance(v))
	})

	t.Run("different values", func(t *testing.T) {
		t.Parallel()
		v := &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(50),
		}
		require.False(t, isVolumeZeroBalance(v))
	})

	t.Run("input nil output set", func(t *testing.T) {
		t.Parallel()
		v := &raftcmdpb.VolumePair{
			Output: commonpb.NewUint256FromUint64(100),
		}
		require.False(t, isVolumeZeroBalance(v))
	})

	t.Run("input set output nil", func(t *testing.T) {
		t.Parallel()
		v := &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(100),
		}
		require.False(t, isVolumeZeroBalance(v))
	})

	t.Run("both zero explicit", func(t *testing.T) {
		t.Parallel()
		v := &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(0),
			Output: commonpb.NewUint256FromUint64(0),
		}
		require.True(t, isVolumeZeroBalance(v))
	})

	t.Run("large equal values all limbs", func(t *testing.T) {
		t.Parallel()
		v := &raftcmdpb.VolumePair{
			Input:  &commonpb.Uint256{V0: 1, V1: 2, V2: 3, V3: 4},
			Output: &commonpb.Uint256{V0: 1, V1: 2, V2: 3, V3: 4},
		}
		require.True(t, isVolumeZeroBalance(v))
	})

	t.Run("differ on high limb", func(t *testing.T) {
		t.Parallel()
		v := &raftcmdpb.VolumePair{
			Input:  &commonpb.Uint256{V0: 1, V1: 2, V2: 3, V3: 4},
			Output: &commonpb.Uint256{V0: 1, V1: 2, V2: 3, V3: 5},
		}
		require.False(t, isVolumeZeroBalance(v))
	})
}

func TestPartitionEphemeralVolumes(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Register a ledger with an ephemeral account type.
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test",
		AccountTypes: map[string]*commonpb.AccountType{
			"clearing": {
				Name:      "clearing",
				Pattern:   "clearing:{id}",
				Ephemeral: true,
			},
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
			},
		},
	}

	// Put ledger into the parent KeyStore so partitionEphemeralVolumes can find it.
	_, _, _, err := machine.Registry.Ledgers.Put(
		(&domain.LedgerKey{Name: "test"}).Bytes(),
		ledgerInfo,
		1,
	)
	require.NoError(t, err)

	buf := &Buffered{
		fsm: machine,
	}

	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			// Ephemeral + zero balance → should be purged
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "clearing:tx1"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "clearing:tx1"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(100),
				Output: commonpb.NewUint256FromUint64(100),
			},
		},
		{
			// Ephemeral + non-zero balance → should be kept
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "clearing:tx2"}, Asset: "EUR"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "clearing:tx2"}, Asset: "EUR"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(200),
				Output: commonpb.NewUint256FromUint64(50),
			},
		},
		{
			// Non-ephemeral + zero balance → should be kept
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "users:alice"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "users:alice"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(0),
				Output: commonpb.NewUint256FromUint64(0),
			},
		},
		{
			// No matching type + zero balance → should be kept
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "unknown:addr"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "unknown:addr"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(50),
				Output: commonpb.NewUint256FromUint64(50),
			},
		},
	}

	result := buf.partitionEphemeralVolumes(updates)

	require.Len(t, result.purged, 1)
	require.Equal(t, "clearing:tx1", result.purged[0].Key.Account)

	require.Len(t, result.kept, 3)
}
