package state

import (
	"errors"
	"io"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func sentinelVolume(account, asset, color string, oldInput, oldOutput, input, output uint64) attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
	key := domain.NewVolumeKey("test", account, asset, color)

	return attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		Key: key, CanonicalKey: key.Bytes(),
		Old: kv.Some(&raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(oldInput), Output: commonpb.NewUint256FromUint64(oldOutput)}),
		New: &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(input), Output: commonpb.NewUint256FromUint64(output)},
	}
}

func sentinelPosting(source, destination, asset, color string, amount int64) *commonpb.Posting {
	posting := commonpb.NewPosting(source, destination, asset, big.NewInt(amount))
	posting.Color = color

	return posting
}

func sentinelLog(revert bool, postings ...*commonpb.Posting) *commonpb.Log {
	transaction := commonpb.NewTransaction().WithPostings(postings...).WithID(1)
	payload := &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: transaction}}}
	if revert {
		payload.Payload = &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{RevertedTransactionId: 0, RevertTransaction: transaction}}
	}

	return &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "test", Log: commonpb.NewLedgerLog(payload)}}}}
}

func TestVerifyVolumeDeltasMatchPostingsRejectsCorruption(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
		want   string
	}{
		{"missing update", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			return u[:1]
		}, "volume delta missing"},
		{"wrong amount", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			u[1].New.Input = commonpb.NewUint256FromUint64(11)

			return u
		}, "volume delta mismatch"},
		{"wrong account", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			u[1].Key.Account = "other"

			return u
		}, "volume delta missing"},
		{"wrong asset", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			u[1].Key = domain.NewVolumeKey("test", "destination", "EUR", "red")

			return u
		}, "volume delta missing"},
		{"wrong color", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			u[1].Key.Color = "blue"

			return u
		}, "volume delta missing"},
		{"wrong ledger", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			u[1].Key.LedgerName = "other"

			return u
		}, "volume delta missing"},
		{"decreasing gross", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			u[1].Old = kv.Some(&raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(20)})

			return u
		}, "volume delta mismatch"},
		{"unexplained nonzero", func(u []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
			return append(u, sentinelVolume("other", "USD", "red", 0, 0, 3, 0))
		}, "unexpected volume delta"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			updates := tc.mutate([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("source", "USD", "red", 0, 0, 0, 10), sentinelVolume("destination", "USD", "red", 0, 0, 10, 0)})
			require.ErrorContains(t, verifyVolumeDeltasMatchPostings(updates, []*commonpb.Log{sentinelLog(false, sentinelPosting("source", "destination", "USD", "red", 10))}), tc.want)
		})
	}
}

// This pair conserves double entry, so only the reverse exact-delta check can reject it.
func TestVerifyVolumeDeltasMatchPostingsRejectsExtraBalancedPair(t *testing.T) {
	t.Parallel()
	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		sentinelVolume("world", "USD", "", 0, 0, 0, 10), sentinelVolume("destination", "USD", "", 0, 0, 10, 0),
		sentinelVolume("unrelated:source", "USD", "", 0, 0, 0, 3), sentinelVolume("unrelated:destination", "USD", "", 0, 0, 3, 0),
	}
	require.NoError(t, checkDoubleEntryInvariant(updates))
	require.ErrorContains(t, verifyVolumeDeltasMatchPostings(updates, []*commonpb.Log{sentinelLog(false, sentinelPosting("world", "destination", "USD", "", 10))}), "unexpected volume delta")
}

func TestVerifyVolumeDeltasMatchPostingsValidScenarios(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
		logs    []*commonpb.Log
	}{
		{"world funding and unchanged touch", []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("world", "USD", "", 0, 0, 0, 10), sentinelVolume("destination", "USD", "", 0, 0, 10, 0), sentinelVolume("touch", "USD", "", 7, 2, 7, 2)}, []*commonpb.Log{sentinelLog(false, sentinelPosting("world", "destination", "USD", "", 10))}},
		{"overlapping bulk and reversal", []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("source", "USD", "", 100, 0, 105, 20), sentinelVolume("destination", "USD", "", 0, 0, 20, 5)}, []*commonpb.Log{sentinelLog(false, sentinelPosting("source", "destination", "USD", "", 10)), sentinelLog(false, sentinelPosting("source", "destination", "USD", "", 10)), sentinelLog(true, sentinelPosting("destination", "source", "USD", "", 5))}},
		{"self transfer", []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("self", "USD", "", 0, 0, 10, 10)}, []*commonpb.Log{sentinelLog(false, sentinelPosting("self", "self", "USD", "", 10))}},
		{"forced overdraft multiple assets and colors", []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("source", "USD", "red", 0, 0, 0, 10), sentinelVolume("destination", "USD", "red", 0, 0, 10, 0), sentinelVolume("source", "USD", "blue", 0, 0, 0, 20), sentinelVolume("destination", "USD", "blue", 0, 0, 20, 0), sentinelVolume("source", "EUR", "red", 0, 0, 0, 30), sentinelVolume("destination", "EUR", "red", 0, 0, 30, 0)}, []*commonpb.Log{sentinelLog(false, sentinelPosting("source", "destination", "USD", "red", 10), sentinelPosting("source", "destination", "USD", "blue", 20), sentinelPosting("source", "destination", "EUR", "red", 30))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, verifyVolumeUpdateMonotonicity(tc.updates))
			require.NoError(t, checkDoubleEntryInvariant(tc.updates))
			require.NoError(t, verifyVolumeDeltasMatchPostings(tc.updates, tc.logs))
		})
	}
}

func TestVerifyVolumeUpdateMonotonicityRejectsDecreasingGross(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		input, output uint64
	}{{"input", 9, 10}, {"output", 10, 9}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, verifyVolumeUpdateMonotonicity([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("account", "USD", "", 10, 10, tc.input, tc.output)}), "volume "+tc.name+" decreased")
		})
	}
}

func TestSentinelMergePreservesLogicalPurgeDeltas(t *testing.T) {
	t.Parallel()
	for _, persistence := range []commonpb.AccountTypePersistence{commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT} {
		t.Run(persistence.String(), func(t *testing.T) {
			t.Parallel()
			buf, machine, store := newTestBuffer(t)
			machine.sentinelMode = true
			machine.sentinelTracer = NewSentinelTracer(machine.logger)
			buf.gatedLedgerTypes = gatedTypesFor(&commonpb.LedgerInfo{Name: "test", AccountTypes: map[string]*commonpb.AccountType{"clearing": {Name: "clearing", Pattern: "clearing:{id}", Persistence: persistence}}})
			updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("world", "USD", "", 0, 0, 0, 10), sentinelVolume("clearing:1", "USD", "", 0, 0, 10, 10), sentinelVolume("destination", "USD", "", 0, 0, 10, 0)}
			for _, update := range updates {
				buf.Volumes().Put(update.Key, update.New)
			}
			batch := store.OpenWriteSession()
			require.NoError(t, buf.Merge(batch, nil))
			require.NoError(t, batch.Commit())
			require.Len(t, buf.AllVolumeUpdates(), 3)
			require.Len(t, buf.KeptVolumeUpdates(), 2)
			require.NoError(t, verifyVolumeDeltasMatchPostings(buf.AllVolumeUpdates(), []*commonpb.Log{sentinelLog(false, sentinelPosting("world", "clearing:1", "USD", "", 10), sentinelPosting("clearing:1", "destination", "USD", "", 10))}))
			key := domain.NewVolumeKey("test", "clearing:1", "USD", "")
			cached, _, err := machine.Registry.Volumes.GetKey(key)
			require.NoError(t, err)
			require.Zero(t, cached.GetInput().ToBigInt().Sign())
			require.Zero(t, cached.GetOutput().ToBigInt().Sign())
			persisted, err := machine.Registry.Attrs.Volume.Get(store, key.Bytes())
			require.NoError(t, err)
			require.Nil(t, persisted)
			require.NoError(t, verifyPostCommitVolumes(store, machine.Registry.Attrs.Volume, buf.KeptVolumeUpdates(), 1, machine.logger))
		})
	}
}

func TestSentinelLaterPurgeWithinBatch(t *testing.T) {
	t.Parallel()
	buf, machine, store := newTestBuffer(t)
	machine.sentinelMode = true
	machine.sentinelTracer = NewSentinelTracer(machine.logger)
	types := gatedTypesFor(&commonpb.LedgerInfo{Name: "test", AccountTypes: map[string]*commonpb.AccountType{"clearing": {Name: "clearing", Pattern: "clearing:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL}}})
	batch := store.OpenWriteSession()
	var results []ApplyResult
	for i, updates := range [][]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{sentinelVolume("world", "USD", "", 0, 0, 0, 10), sentinelVolume("clearing:1", "USD", "", 0, 0, 10, 0)},
		{sentinelVolume("clearing:1", "USD", "", 10, 0, 10, 10), sentinelVolume("destination", "USD", "", 0, 0, 10, 0)},
	} {
		buf.Reset(&commonpb.Timestamp{Data: uint64(i + 1)})
		buf.gatedLedgerTypes = types
		for _, update := range updates {
			buf.Volumes().Put(update.Key, update.New)
		}
		require.NoError(t, buf.Merge(batch, nil))
		source, destination := "world", "clearing:1"
		if i == 1 {
			source, destination = "clearing:1", "destination"
		}
		require.NoError(t, verifyVolumeDeltasMatchPostings(buf.AllVolumeUpdates(), []*commonpb.Log{sentinelLog(false, sentinelPosting(source, destination, "USD", "", 10))}))
		results = append(results, ApplyResult{volumeUpdates: buf.KeptVolumeUpdates(), purgedVolumeKeys: buf.PurgedVolumeKeys()})
	}
	require.NoError(t, batch.Commit())
	deduped := deduplicateVolumeUpdates(results)
	require.Len(t, deduped, 2)
	require.NoError(t, verifyPostCommitVolumes(store, machine.Registry.Attrs.Volume, deduped, 2, machine.logger))
}

func TestVerifyPostCommitVolumes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		persisted *raftcmdpb.VolumePair
		want      string
	}{
		{"missing", nil, "volume missing from pebble after commit"},
		{"matching", sentinelVolume("a", "USD", "", 0, 0, 10, 0).New, ""},
		{"mismatched", sentinelVolume("a", "USD", "", 0, 0, 11, 0).New, "cache/pebble volume divergence"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, machine, store := newTestBuffer(t)
			update := sentinelVolume("account", "USD", "red", 0, 0, 10, 0)
			if tc.persisted != nil {
				batch := store.OpenWriteSession()
				_, err := machine.Registry.Attrs.Volume.Set(batch, update.CanonicalKey, tc.persisted)
				require.NoError(t, err)
				require.NoError(t, batch.Commit())
			}
			err := verifyPostCommitVolumes(store, machine.Registry.Attrs.Volume, []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{update}, 42, machine.logger)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

// PebbleGetter has no generated mock; this function adapter injects the exact
// read failure without conflating it with Pebble's not-found response.
type sentinelFailingGetter func([]byte) ([]byte, io.Closer, error)

func (get sentinelFailingGetter) Get(key []byte) ([]byte, io.Closer, error) {
	return get(key)
}

func TestVerifyPostCommitVolumesReadFailure(t *testing.T) {
	t.Parallel()
	_, machine, _ := newTestBuffer(t)
	failure := errors.New("injected pebble read failure")
	getter := sentinelFailingGetter(func([]byte) ([]byte, io.Closer, error) { return nil, nil, failure })
	err := verifyPostCommitVolumes(getter, machine.Registry.Attrs.Volume, []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("account", "USD", "", 0, 0, 10, 0)}, 42, machine.logger)
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "reading volume from pebble for verification")
	require.NotContains(t, err.Error(), "volume missing")
}

// Two offenders that differ only by color are the case the deterministic
// representative exists for: map iteration would otherwise name either one,
// and a message without the color could not tell which was named. Running the
// same input repeatedly must produce one message, and that message must be the
// lowest-sorting offender's.
func TestVerifyVolumeDeltasMatchPostingsNamesTheLowestColorDeterministically(t *testing.T) {
	t.Parallel()

	// Two colors of the same (account, asset) pair, both debited and credited,
	// so the log explains neither: both become unexplained offenders.
	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		sentinelVolume("source", "USD", "red", 0, 0, 0, 7),
		sentinelVolume("destination", "USD", "red", 0, 0, 7, 0),
		sentinelVolume("source", "USD", "blue", 0, 0, 0, 7),
		sentinelVolume("destination", "USD", "blue", 0, 0, 7, 0),
	}

	first := verifyVolumeDeltasMatchPostings(updates, nil)
	require.Error(t, first)
	require.ErrorContains(t, first, "unexpected volume delta")
	require.ErrorContains(t, first, "(4 offending keys)")

	// "blue" sorts before "red" under compareVolumeKeys, and "destination"
	// before "source", so exactly one of the four can be named.
	require.ErrorContains(t, first, `"test"/destination/USD/blue`)
	require.NotContains(t, first.Error(), "/red", "only one offender may be named")

	for range 20 {
		require.Equal(t, first.Error(), verifyVolumeDeltasMatchPostings(updates, nil).Error(),
			"map iteration must not change which offender is named")
	}
}
