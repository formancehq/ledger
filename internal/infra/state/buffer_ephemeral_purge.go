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

// ephemeralPurgeResult holds the result of partitioning volume updates.
type ephemeralPurgeResult struct {
	kept   []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
	purged []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]
}

// partitionEphemeralVolumes splits volume updates into kept and purged sets.
// A volume is purged when it has zero balance (input == output) and its account
// matches an ephemeral account type.
func (b *Buffered) partitionEphemeralVolumes(
	updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) ephemeralPurgeResult {
	// Build a cache of ledger → account types to avoid repeated lookups.
	ledgerTypes := make(map[string]map[string]*commonpb.AccountType)

	result := ephemeralPurgeResult{
		kept: make([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], 0, len(updates)),
	}

	for _, update := range updates {
		if !isVolumeZeroBalance(update.New) {
			result.kept = append(result.kept, update)

			continue
		}

		types, ok := ledgerTypes[update.Key.Ledger]
		if !ok {
			info, _, err := b.fsm.Registry.Ledgers.Get(
				domain.LedgerKey{Name: update.Key.Ledger}.Bytes(),
			)
			if err != nil || info == nil {
				result.kept = append(result.kept, update)

				continue
			}

			types = info.GetAccountTypes()
			ledgerTypes[update.Key.Ledger] = types
		}

		if len(types) == 0 {
			result.kept = append(result.kept, update)

			continue
		}

		matched := accounttype.FindMatchingType(update.Key.Account, types)
		if matched == nil || !matched.GetEphemeral() {
			result.kept = append(result.kept, update)

			continue
		}

		result.purged = append(result.purged, update)
	}

	return result
}

// applyEphemeralPurge deletes purged volume entries from Pebble and the parent KeyStore.
func (b *Buffered) applyEphemeralPurge(
	batch *dal.Batch,
	purged []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) error {
	for _, update := range purged {
		// Delete the entry from Pebble.
		if err := b.attrs.Volume.Delete(batch, update.CanonicalKey); err != nil {
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
