package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/kv"
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
	_, _, err := machine.Registry.Ledgers.KeyStore().Put(
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

	_, _, err := machine.Registry.Ledgers.KeyStore().Put(
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

// TestPartitionVolumesTransient_PreExistingBalance covers the ephemeral-mirror
// lifecycle for an account that gained a non-zero balance under a default-normal
// policy before its pattern was matched by a TRANSIENT type. As long as the
// running cumulative stays unbalanced, the update is kept (0xF1 reflects the
// running cumulative). Once a batch brings it back to zero balance, the update
// is purged (0xF1 deleted), and from then on the account behaves as steady-state
// transient.
func TestPartitionVolumesTransient_PreExistingBalance(t *testing.T) {
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
		},
	}

	_, _, err := machine.Registry.Ledgers.KeyStore().Put(
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

	preExistingUnbalanced := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(0),
	}

	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			// TRANSIENT + Old non-zero, New still non-zero → kept (draining phase).
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:draining"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:draining"}, Asset: "USD"}).Bytes(),
			Old:          kv.Some(preExistingUnbalanced),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(100),
				Output: commonpb.NewUint256FromUint64(50),
			},
		},
		{
			// TRANSIENT + Old non-zero, New zero-balance → purged (rebalance).
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:rebalanced"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:rebalanced"}, Asset: "USD"}).Bytes(),
			Old:          kv.Some(preExistingUnbalanced),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(100),
				Output: commonpb.NewUint256FromUint64(100),
			},
		},
		{
			// TRANSIENT + Old already zero-balance → steady-state transient.
			Key:          domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:steady"}, Asset: "USD"},
			CanonicalKey: (&domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:steady"}, Asset: "USD"}).Bytes(),
			Old: kv.Some(&raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(0),
				Output: commonpb.NewUint256FromUint64(0),
			}),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(42),
				Output: commonpb.NewUint256FromUint64(42),
			},
		},
	}

	result := buf.partitionVolumes(updates)

	require.Len(t, result.kept, 1)
	require.Equal(t, "staging:draining", result.kept[0].Key.Account)

	require.Len(t, result.purged, 1)
	require.Equal(t, "staging:rebalanced", result.purged[0].Key.Account)

	require.Len(t, result.transient, 1)
	require.Equal(t, "staging:steady", result.transient[0].Key.Account)
}

// TestZeroVolumeCache_OverwritesKeyStore guards the invariant that the in-memory
// KeyStore (b.fsm.Registry.Volumes) ends up with {0, 0} after zeroVolumeCache,
// regardless of the cumulative value that was Put before it ran. Without this
// behaviour, a transient cell would silently carry its prior cumulative value
// into the next batch's GetVolume → PCV, drifting away from the documented
// "never persisted, must be zero at end of batch" semantic.
func TestZeroVolumeCache_OverwritesKeyStore(t *testing.T) {
	t.Parallel()

	buf, machine := newTestBuffer(t)

	keyA := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:tx1"}, Asset: "USD"}
	keyB := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "staging:tx2"}, Asset: "EUR"}

	// Simulate DerivedKeyStore.Merge having already written the cumulative value
	// into the parent KeyStore. Pre-fix, this state would survive the cache write
	// step and leak into the next batch.
	cumulativeA := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(100),
	}
	cumulativeB := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(50),
		Output: commonpb.NewUint256FromUint64(50),
	}

	_, _, err := machine.Registry.Volumes.KeyStore().Put(keyA.Bytes(), cumulativeA)
	require.NoError(t, err)
	_, _, err = machine.Registry.Volumes.KeyStore().Put(keyB.Bytes(), cumulativeB)
	require.NoError(t, err)

	// Sanity-check the precondition: the cumulative value is what GetKey returns.
	got, _, err := machine.Registry.Volumes.GetKey(keyA)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got.GetInput().GetV0())
	require.Equal(t, uint64(100), got.GetOutput().GetV0())

	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			Key:          keyA,
			CanonicalKey: keyA.Bytes(),
			New:          cumulativeA,
		},
		{
			Key:          keyB,
			CanonicalKey: keyB.Bytes(),
			New:          cumulativeB,
		},
	}

	batch := machine.dataStore.NewBatch()
	require.NoError(t, buf.zeroVolumeCache(batch, 0, updates))
	require.NoError(t, batch.Commit())

	// After the call, both keys must read {0, 0} from the in-memory KeyStore.
	for _, key := range []domain.VolumeKey{keyA, keyB} {
		v, _, err := machine.Registry.Volumes.GetKey(key)
		require.NoError(t, err)
		require.True(t, isVolumeZeroBalance(v), "expected zero-balance KeyStore entry for %s", key.Account)
		require.Equal(t, uint64(0), v.GetInput().GetV0())
		require.Equal(t, uint64(0), v.GetOutput().GetV0())
	}
}

// TestZeroVolumeCache_Empty is a no-op early-return guard so callers can hand
// it an empty slice without an allocated batch write.
func TestZeroVolumeCache_Empty(t *testing.T) {
	t.Parallel()

	buf, machine := newTestBuffer(t)
	batch := machine.dataStore.NewBatch()

	require.NoError(t, buf.zeroVolumeCache(batch, 0, nil))
	require.NoError(t, batch.Commit())
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
