package state

import (
	"errors"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
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
	kept      []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // NORMAL + non-zero ephemeral
	purged    []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // EPHEMERAL with zero balance
	transient []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // TRANSIENT — never written to Pebble
}

// partitionVolumes splits volume updates into kept, purged, and transient sets.
//
//   - NORMAL accounts: always kept
//   - EPHEMERAL accounts with zero balance: purged (deleted from Pebble)
//   - EPHEMERAL accounts with non-zero balance: kept
//   - TRANSIENT accounts: always transient (never written to Pebble)
func (b *Buffered) partitionVolumes(
	updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) volumePartitionResult {
	// Build a cache of ledger → compiled account types to avoid repeated parsing.
	ledgerTypes := make(map[string][]accounttype.CompiledType)

	result := volumePartitionResult{
		kept: make([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], 0, len(updates)),
	}

	for _, update := range updates {
		compiled, ok := ledgerTypes[update.Key.Ledger]
		if !ok {
			info, _, err := b.fsm.Registry.Ledgers.Get(
				domain.LedgerKey{Name: update.Key.Ledger}.Bytes(),
			)
			if err != nil || info == nil {
				result.kept = append(result.kept, update)

				continue
			}

			compiled = accounttype.CompileTypes(info.GetAccountTypes())
			ledgerTypes[update.Key.Ledger] = compiled
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
			// Transient behavior only activates when the base volume is zero (or absent).
			// Pre-existing non-zero volumes (from before the account was marked transient)
			// are treated as normal to avoid losing persisted data.
			if update.Old.IsDefined() && !isVolumeZeroBalance(update.Old.Value()) {
				result.kept = append(result.kept, update)
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

// evictTransientVolumes removes transient volumes from the in-memory parent KeyStore.
// Unlike ephemeral purge, transient volumes were never written to Pebble, so only
// the in-memory eviction is needed.
func (b *Buffered) evictTransientVolumes(
	transient []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) {
	for _, update := range transient {
		_, _ = b.fsm.Registry.Volumes.Delete(update.CanonicalKey)
	}
}

// applyEphemeralPurge deletes purged volume entries from Pebble, the parent KeyStore,
// and the 0xFF cache zone.
func (b *Buffered) applyEphemeralPurge(
	batch *dal.Batch,
	genByte byte,
	purged []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) error {
	for _, update := range purged {
		// Delete the entry from Pebble attributes zone.
		if err := b.attrs.Volume.Delete(batch, update.CanonicalKey); err != nil {
			return err
		}

		// Delete from 0xFF cache zone.
		if err := deleteCacheEntry(batch, genByte, dal.AttributePrefixVolume, update.ID); err != nil {
			return err
		}

		// Evict from the parent KeyStore so the cache doesn't keep stale zero-balance entries.
		if _, err := b.fsm.Registry.Volumes.Delete(update.CanonicalKey); err != nil {
			// Ignore not-found — the entry may have been evicted by cache rotation.
			if !errors.Is(err, domain.ErrNotFound) {
				return err
			}
		}
	}

	return nil
}
