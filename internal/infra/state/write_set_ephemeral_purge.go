package state

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// isVolumeZeroBalance returns true when input == output (all 4 limbs match).
func isVolumeZeroBalance(v *raftcmdpb.VolumePair) bool {
	in := v.GetInput()
	out := v.GetOutput()

	if in == nil && out == nil {
		return true
	}

	if in == nil || out == nil {
		return false
	}

	return in.GetV0() == out.GetV0() &&
		in.GetV1() == out.GetV1() &&
		in.GetV2() == out.GetV2() &&
		in.GetV3() == out.GetV3()
}

// volumePartitionResult holds the result of partitioning volume updates by persistence mode.
type volumePartitionResult struct {
	kept      []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // NORMAL + non-zero ephemeral + draining-transient
	purged    []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // EPHEMERAL or draining-TRANSIENT once back to zero balance
	transient []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // steady-state TRANSIENT — never written to Pebble
}

// partitionVolumes splits volume updates into kept, purged, and transient sets.
//
//   - NORMAL accounts: always kept
//   - EPHEMERAL accounts with zero balance: purged (deleted from Pebble)
//   - EPHEMERAL accounts with non-zero balance: kept
//   - TRANSIENT accounts with a pre-existing non-zero balance (from before the
//     transient pattern started matching them): mirror EPHEMERAL — kept while
//     the running cumulative is still unbalanced, purged once it returns to
//     zero balance. Steady-state TRANSIENT (no pre-existing balance, or already
//     purged): never written to Pebble.
func (b *WriteSet) partitionVolumes(
	updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) volumePartitionResult {
	// Build a cache of ledger → compiled account types to avoid repeated parsing.
	ledgerTypes := make(map[string][]accounttype.CompiledType)

	result := volumePartitionResult{
		kept: make([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], 0, len(updates)),
	}

	for _, update := range updates {
		compiled, ok := ledgerTypes[update.Key.LedgerName]
		if !ok {
			info, err := b.getLedgerData(update.Key.LedgerName)
			if err != nil {
				result.kept = append(result.kept, update)

				continue
			}

			compiled = accounttype.CompileTypes(info.GetAccountTypes())
			ledgerTypes[update.Key.LedgerName] = compiled
		}

		if len(compiled) == 0 {
			result.kept = append(result.kept, update)

			continue
		}

		matched := accounttype.FindMatchingType(update.Key.Account, compiled)
		if matched == nil {
			result.kept = append(result.kept, update)

			continue
		}

		switch matched.GetPersistence() {
		case commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT:
			// Pre-existing non-zero balance (e.g., account was funded under a
			// default-normal policy before the transient pattern started matching
			// it): mirror the ephemeral lifecycle. Keep the running cumulative in
			// 0xF1 while it's still unbalanced; purge once it returns to zero
			// balance. Once purged, KS.M is zeroed and the account behaves as
			// steady-state transient (Old.IsZero) from then on.
			if update.Old.IsDefined() && !isVolumeZeroBalance(update.Old.Value()) {
				if isVolumeZeroBalance(update.New) {
					result.purged = append(result.purged, update)
				} else {
					result.kept = append(result.kept, update)
				}
			} else {
				result.transient = append(result.transient, update)
			}

		case commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL:
			if isVolumeZeroBalance(update.New) {
				result.purged = append(result.purged, update)
			} else {
				result.kept = append(result.kept, update)
			}

		default:
			result.kept = append(result.kept, update)
		}
	}

	return result
}

// applyEphemeralPurge deletes purged volumes from 0xF1 then zeroes the cache.
// Deleting saves storage; the cache is zeroed (rather than deleted) so any
// co-batched proposal admitted with CacheHit still sees a populated
// entry.
func (b *WriteSet) applyEphemeralPurge(
	batch *dal.WriteSession,
	genByte byte,
	purged []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) error {
	if len(purged) == 0 {
		return nil
	}

	for _, update := range purged {
		if err := b.attrs.Volume.Delete(batch, update.CanonicalKey); err != nil {
			return err
		}
	}

	return b.zeroVolumeCache(batch, genByte, purged)
}

// zeroVolumeCache overwrites the in-memory KeyStore and the 0xFF cache zone
// with a zero VolumePair for each update. It does NOT touch 0xF1 — callers
// that need a Pebble delete must do it themselves before invoking this.
//
// Used by:
//   - applyEphemeralPurge after deleting the persistent entry.
//   - the transient flow, which never writes the persistent entry but still
//     needs the cache populated with zero so that the next batch's GetVolume
//     reads {0, 0} rather than the prior cumulative value, and so cache
//     restore after restart honours the documented "never persisted, must be
//     zero at end of batch" semantic.
//
// The zero entry ages out via cache generation rotation.
func (b *WriteSet) zeroVolumeCache(
	batch *dal.WriteSession,
	genByte byte,
	updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) error {
	if len(updates) == 0 {
		return nil
	}

	zeroBytes, err := (&raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}).MarshalVT()
	if err != nil {
		return err
	}

	for _, update := range updates {
		// Allocate a fresh zero VolumePair per entry to avoid shared-pointer
		// mutations leaking across keys.
		zeroVol := &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(0),
			Output: commonpb.NewUint256FromUint64(0),
		}
		if err := b.fsm.Registry.Volumes.PutCacheOnly(batch, genByte, update.CanonicalKey, zeroVol, zeroBytes); err != nil {
			return err
		}
	}

	return nil
}
