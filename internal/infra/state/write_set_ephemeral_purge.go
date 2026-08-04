package state

import (
	"fmt"

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
// Account types are read from b.gatedLedgerTypes, which ValidateTransientVolumes
// resolves through the gated Scope before Merge runs. partitionVolumes performs
// no ledger read of its own: it executes at Merge time, past the per-order
// coverage gate, so a direct Derived.Ledgers read here would classify volumes
// off keys the proposal never declared (invariant #9). A ledger missing from the
// map is an invariant violation, not an expected miss — see the hard fail below.
//
//   - NORMAL accounts: always kept
//   - EPHEMERAL accounts with zero balance: purged (deleted from Pebble)
//   - EPHEMERAL accounts with non-zero balance: kept
//   - TRANSIENT accounts with a persisted row (non-zero Old, from before the
//     transient pattern started matching them): mirror EPHEMERAL — kept while
//     the running cumulative is still unbalanced, purged once it is at zero
//     balance. Steady-state TRANSIENT (all-zero Old: never persisted, or
//     already purged): never written to Pebble.
func (b *WriteSet) partitionVolumes(
	updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) (volumePartitionResult, error) {
	result := volumePartitionResult{
		kept: make([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], 0, len(updates)),
	}

	for _, update := range updates {
		// Account types come from ValidateTransientVolumes' gated resolution,
		// never from a raw Derived.Ledgers read: the classification below picks
		// which volumes are written to Pebble and which are purged, so it must
		// be driven by proposal-declared, coverage-gated data (invariant #9).
		entry, ok := b.gatedLedgerTypes[update.Key.LedgerName]
		if !ok {
			// Impossible by design: ValidateTransientVolumes runs before Merge
			// on the order path and covers exactly this key set (it walks the
			// same DirtyValues() map Merge drains), and only order handlers
			// write volumes — so a technical-only proposal reaches Merge with
			// none. A miss therefore means the gated pass did not run over this
			// key, and silently defaulting to "kept" would let an ungated
			// classification decide Pebble contents (invariant #7).
			return volumePartitionResult{}, fmt.Errorf(
				"invariant: volume update for ledger %q whose account types were never resolved through the gated scope",
				update.Key.LedgerName,
			)
		}

		if !entry.found {
			// The gate resolved this ledger as genuinely absent — it carries no
			// account-type info (never created). Default persistence is "kept".
			// A ledger deleted earlier in this batch does NOT land here:
			// DeleteLedger soft-deletes by Putting the row back with DeletedAt
			// set, so the gated read still returns it with its types intact.
			result.kept = append(result.kept, update)

			continue
		}

		compiled := entry.compiled

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
			// A defined Old with non-zero limbs means a persisted row exists
			// from before the transient pattern started matching the account
			// (funded under a default-normal policy — its balance may already
			// sit at zero, e.g. after a revert). Mirror the ephemeral
			// lifecycle: keep the running cumulative in 0xF1 while it's still
			// unbalanced; purge once it is at zero balance, deleting the row.
			// An all-zero Old (post-purge zeroed cache entry, or the
			// preloader's zero seed for a fresh key) is steady-state
			// transient: nothing persisted, nothing to delete.
			if update.Old.IsDefined() && !isVolumePreloadZero(update.Old.Value()) {
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

	return result, nil
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
