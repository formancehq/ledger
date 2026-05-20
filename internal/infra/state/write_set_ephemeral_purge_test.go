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
		Id:   1,
		AccountTypes: map[string]*commonpb.AccountType{
			"clearing": {
				Name:        "clearing",
				Pattern:     "clearing:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
			},
		},
	}

	// Put ledger into the parent KeyStore so partitionEphemeralVolumes can find it.
	_, _, err := machine.Registry.Ledgers.Put(
		(&domain.LedgerKey{Name: "test"}).Bytes(),
		ledgerInfo,
	)
	require.NoError(t, err)

	derived := NewDerivedRegistry(machine.Registry)
	derived.Ledgers.Put(domain.LedgerKey{Name: "test"}, ledgerInfo)

	buf := &WriteSet{
		fsm:     machine,
		Derived: derived,
	}

	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			// Ephemeral + zero balance → should be purged
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "clearing:tx1"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "clearing:tx1"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(100),
				Output: commonpb.NewUint256FromUint64(100),
			},
		},
		{
			// Ephemeral + non-zero balance → should be kept
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "clearing:tx2"}, Asset: "EUR"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "clearing:tx2"}, Asset: "EUR"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(200),
				Output: commonpb.NewUint256FromUint64(50),
			},
		},
		{
			// Non-ephemeral + zero balance → should be kept
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "users:alice"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "users:alice"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(0),
				Output: commonpb.NewUint256FromUint64(0),
			},
		},
		{
			// No matching type + zero balance → should be kept
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "unknown:addr"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "unknown:addr"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(50),
				Output: commonpb.NewUint256FromUint64(50),
			},
		},
	}

	result := buf.partitionVolumes(updates)

	require.Len(t, result.purged, 1)
	require.Equal(t, "clearing:tx1", result.purged[0].Key.Account)

	require.Len(t, result.kept, 3)
	require.Empty(t, result.transient)
}

func TestPartitionVolumesTransient(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test",
		Id:   1,
		AccountTypes: map[string]*commonpb.AccountType{
			"staging": {
				Name:        "staging",
				Pattern:     "staging:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
			},
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
			},
		},
	}

	_, _, err := machine.Registry.Ledgers.Put(
		(&domain.LedgerKey{Name: "test"}).Bytes(),
		ledgerInfo,
	)
	require.NoError(t, err)

	derived := NewDerivedRegistry(machine.Registry)
	derived.Ledgers.Put(domain.LedgerKey{Name: "test"}, ledgerInfo)

	buf := &WriteSet{
		fsm:     machine,
		Derived: derived,
	}

	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			// Transient + zero balance → transient partition
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:tx1"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:tx1"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(100),
				Output: commonpb.NewUint256FromUint64(100),
			},
		},
		{
			// Transient + non-zero balance → transient partition (validation catches this later)
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:tx2"}, Asset: "EUR"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:tx2"}, Asset: "EUR"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(200),
				Output: commonpb.NewUint256FromUint64(50),
			},
		},
		{
			// Normal account → kept
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "users:alice"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "users:alice"}, Asset: "USD"}).Bytes(),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(100),
				Output: commonpb.NewUint256FromUint64(0),
			},
		},
	}

	result := buf.partitionVolumes(updates)

	require.Len(t, result.transient, 2)
	require.Len(t, result.kept, 1)
	require.Equal(t, "users:alice", result.kept[0].Key.Account)
	require.Empty(t, result.purged)
}

func TestIsVolumeZeroBalance_Transient(t *testing.T) {
	t.Parallel()

	t.Run("equal input/output → zero balance", func(t *testing.T) {
		t.Parallel()

		vol := &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(100),
		}
		require.True(t, isVolumeZeroBalance(vol))
	})

	t.Run("unequal input/output → non-zero balance", func(t *testing.T) {
		t.Parallel()

		vol := &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(50),
		}
		require.False(t, isVolumeZeroBalance(vol))
	})
}
