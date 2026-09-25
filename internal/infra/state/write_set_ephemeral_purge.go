package state

import (
	"errors"
	"fmt"
	"sort"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// PrepareEphemeralAccountPurge decides account liveness from the complete
// admission-declared volume set. It runs under the proposal-wide coverage gate
// after all orders have staged their effects and before Merge drains overlays.
func (b *WriteSet) PrepareEphemeralAccountPurge(scope processing.Scope, plans []*raftcmdpb.AttributeCoverage) error {
	candidates := make(map[domain.AccountKey]struct{})
	for key := range b.Derived.Volumes.DirtyValues() {
		candidates[key.AccountKey] = struct{}{}
	}
	for key := range b.Derived.AccountMetadata.DirtyValues() {
		candidates[key.AccountKey] = struct{}{}
	}
	for key := range b.Derived.AccountMetadata.DirtyDeletions() {
		candidates[key.AccountKey] = struct{}{}
	}

	volumeKeys := make(map[domain.AccountKey][]domain.VolumeKey)
	persistedVolumes := make(map[domain.VolumeKey]struct{})
	metadataKeys := make(map[domain.AccountKey][]domain.MetadataKey)
	for _, plan := range plans {
		if len(plan.GetCanonicalKey()) == 0 {
			switch byte(plan.GetAttrCode()) {
			case dal.SubAttrVolume, dal.SubAttrMetadata:
				return fmt.Errorf("covered lifecycle attribute 0x%02x has no canonical key", plan.GetAttrCode())
			}

			continue
		}
		switch byte(plan.GetAttrCode()) {
		case dal.SubAttrVolume:
			var key domain.VolumeKey
			if err := key.Unmarshal(plan.GetCanonicalKey()); err != nil {
				return fmt.Errorf("decoding covered volume key: %w", err)
			}
			volumeKeys[key.AccountKey] = append(volumeKeys[key.AccountKey], key)
			if plan.GetPersisted() {
				persistedVolumes[key] = struct{}{}
			}
			if plan.GetLifecycleCandidate() {
				candidates[key.AccountKey] = struct{}{}
			}
		case dal.SubAttrMetadata:
			var key domain.MetadataKey
			if err := key.Unmarshal(plan.GetCanonicalKey()); err != nil {
				return fmt.Errorf("decoding covered metadata key: %w", err)
			}
			metadataKeys[key.AccountKey] = append(metadataKeys[key.AccountKey], key)
			if plan.GetLifecycleCandidate() {
				candidates[key.AccountKey] = struct{}{}
			}
		}
	}

	b.purgedAccounts = make(map[domain.AccountKey]struct{})
	b.purgedAccountVolumeKeys = b.purgedAccountVolumeKeys[:0]
	b.purgedAccountMetadataKeys = b.purgedAccountMetadataKeys[:0]
	for account := range candidates {
		ephemeral, err := b.isEphemeralAccount(scope, account)
		if err != nil {
			return err
		}
		if !ephemeral {
			continue
		}
		live := false
		persistedVolumeKeys := make([]domain.VolumeKey, 0, len(volumeKeys[account]))
		for _, key := range volumeKeys[account] {
			volume, err := scope.Volumes().Get(key)
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("reading covered volume for account purge: %w", err)
			}
			pair := volume.Mutate()
			if !isVolumeZeroBalance(pair) {
				live = true

				break
			}
			// A successful read proves this is a persisted or in-batch row. Absent
			// coverage placeholders are normalized to ErrNotFound by the accessor.
			if _, persisted := persistedVolumes[key]; persisted {
				persistedVolumeKeys = append(persistedVolumeKeys, key)
			}
		}
		if live {
			continue
		}
		b.purgedAccounts[account] = struct{}{}
		b.purgedAccountVolumeKeys = append(b.purgedAccountVolumeKeys, persistedVolumeKeys...)
		b.purgedAccountMetadataKeys = append(b.purgedAccountMetadataKeys, metadataKeys[account]...)
	}

	sort.Slice(b.purgedAccountVolumeKeys, func(i, j int) bool {
		return string(b.purgedAccountVolumeKeys[i].Bytes()) < string(b.purgedAccountVolumeKeys[j].Bytes())
	})
	sort.Slice(b.purgedAccountMetadataKeys, func(i, j int) bool {
		return string(b.purgedAccountMetadataKeys[i].Bytes()) < string(b.purgedAccountMetadataKeys[j].Bytes())
	})

	return nil
}

func (b *WriteSet) isEphemeralAccount(scope processing.Scope, key domain.AccountKey) (bool, error) {
	entry, ok := b.gatedLedgerTypes[key.LedgerName]
	if !ok {
		info, err := scope.Ledgers().Get(domain.LedgerKey{Name: key.LedgerName})
		if errors.Is(err, domain.ErrNotFound) {
			b.gatedLedgerTypes[key.LedgerName] = gatedLedgerType{}

			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("loading ledger for account purge: %w", err)
		}
		entry = gatedLedgerType{compiled: accounttype.CompileTypes(info.Mutate().GetAccountTypes()), found: true}
		b.gatedLedgerTypes[key.LedgerName] = entry
	}
	if !entry.found {
		return false, nil
	}
	matched := accounttype.FindMatchingType(key.Account, entry.compiled)

	return matched != nil && matched.GetPersistence() == commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, nil
}

func (b *WriteSet) stagePurgedAccountRows() error {
	dirtyVolumes := b.Derived.Volumes.DirtyValues()
	for _, key := range b.purgedAccountVolumeKeys {
		if _, touched := dirtyVolumes[key]; touched {
			continue
		}
		b.Derived.Volumes.Delete(key)
	}
	for _, key := range b.purgedAccountMetadataKeys {
		b.Derived.AccountMetadata.Delete(key)
	}

	return nil
}

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
	kept           []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // NORMAL + non-zero ephemeral + draining-transient
	purged         []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // EPHEMERAL or draining-TRANSIENT once back to zero balance
	transient      []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // steady-state TRANSIENT — never written to Pebble
	transientPurge []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] // grandfathered TRANSIENT rows deleted on drain
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
					result.transientPurge = append(result.transientPurge, update)
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

func (b *WriteSet) applyCoveredVolumeDeletions(
	batch *dal.WriteSession,
	genByte byte,
	deletions []attributes.Deletion[domain.VolumeKey],
) error {
	if len(deletions) == 0 {
		return nil
	}
	updates := make([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], 0, len(deletions))
	for _, deletion := range deletions {
		if err := b.attrs.Volume.Delete(batch, deletion.CanonicalKey); err != nil {
			return err
		}
		updates = append(updates, attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
			Key: deletion.Key, ID: deletion.ID, Tag: deletion.Tag, CanonicalKey: deletion.CanonicalKey,
		})
	}

	return b.zeroVolumeCache(batch, genByte, updates)
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
