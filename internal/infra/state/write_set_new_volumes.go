package state

import (
	"sort"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// isVolumePreloadZero returns true if the volume pair is the zero placeholder
// injected by the preloader for keys that don't exist in Pebble. Unlike
// isVolumeZeroBalance (input == output), this checks input == 0 AND output == 0
// — the exact seed the preloader emits so admission's `Needs` can be planned
// deterministically.
func isVolumePreloadZero(v *raftcmdpb.VolumePair) bool {
	if v == nil {
		return true
	}

	in := v.GetInput()
	out := v.GetOutput()

	return (in == nil || (in.GetV0() == 0 && in.GetV1() == 0 && in.GetV2() == 0 && in.GetV3() == 0)) &&
		(out == nil || (out.GetV0() == 0 && out.GetV1() == 0 && out.GetV2() == 0 && out.GetV3() == 0))
}

// newPersistentVolumeKey is the (ledger, account, asset) tuple used by makeNewKeySet
// and buildNewByLog. Mirrors purgedVolumeKey — see write_set_ephemeral_purge.go
// for the asset-dimension rationale.
type newPersistentVolumeKey struct {
	Ledger  string
	Account string
	Asset   string
}

// makeNewKeySet builds a lookup set over the (ledger, account, asset) of every
// PERSISTENT volume that was newly created by this proposal. A volume is
// "new" iff its preloaded prior value was either undefined or the zero
// placeholder — i.e. the attribute store did not yet carry an entry for that
// (account, asset) tuple.
//
// Only persistent updates (kept + ephemeral) qualify. Transient volumes never
// hit the attribute store, so they are never "new" in the persisted sense —
// the caller passes only persistent slices.
func makeNewKeySet(persistentUpdates ...[]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) map[newPersistentVolumeKey]struct{} {
	total := 0
	for _, updates := range persistentUpdates {
		total += len(updates)
	}

	if total == 0 {
		return nil
	}

	set := make(map[newPersistentVolumeKey]struct{}, total)

	for _, updates := range persistentUpdates {
		for i := range updates {
			old := updates[i].Old
			if old.IsDefined() && !isVolumePreloadZero(old.Value()) {
				continue
			}

			set[newPersistentVolumeKey{
				Ledger:  updates[i].Key.LedgerName,
				Account: updates[i].Key.Account,
				Asset:   updates[i].Key.Asset,
			}] = struct{}{}
		}
	}

	return set
}

// buildNewByLog produces, for each order index, the deduplicated list of
// (account, asset) tuples that the order touched and that the proposal
// classified as newly-created persistent volumes. Indexed by order_index;
// entries for orders that produced no new persistent volume are nil. Tuples
// within an entry are sorted (by account then asset) to keep the log payload
// deterministic across runs.
//
// Semantically parallel to buildPurgedByLog. Ephemeral volumes appear in
// BOTH new_volumes AND purged_volumes for the same log: they were persisted
// (hence "new") and evicted after commit (hence "purged"). The usagebuilder's
// VolumeCount = sum(new_volumes) - sum(purged_volumes) relies on that
// symmetry.
func buildNewByLog(perOrderVolumeKeys [][]domain.VolumeKey, newSet map[newPersistentVolumeKey]struct{}) [][]*commonpb.TouchedVolume {
	if len(perOrderVolumeKeys) == 0 || len(newSet) == 0 {
		return nil
	}

	type accAsset struct{ Account, Asset string }

	out := make([][]*commonpb.TouchedVolume, len(perOrderVolumeKeys))
	for i, keys := range perOrderVolumeKeys {
		if len(keys) == 0 {
			continue
		}

		seen := make(map[accAsset]struct{}, len(keys))
		for _, k := range keys {
			if _, ok := newSet[newPersistentVolumeKey{Ledger: k.LedgerName, Account: k.Account, Asset: k.Asset}]; !ok {
				continue
			}
			seen[accAsset{Account: k.Account, Asset: k.Asset}] = struct{}{}
		}

		if len(seen) == 0 {
			continue
		}

		ordered := make([]accAsset, 0, len(seen))
		for k := range seen {
			ordered = append(ordered, k)
		}
		sort.Slice(ordered, func(a, b int) bool {
			if ordered[a].Account != ordered[b].Account {
				return ordered[a].Account < ordered[b].Account
			}

			return ordered[a].Asset < ordered[b].Asset
		})

		vols := make([]*commonpb.TouchedVolume, len(ordered))
		for j, k := range ordered {
			vols[j] = &commonpb.TouchedVolume{Account: k.Account, Asset: k.Asset}
		}
		out[i] = vols
	}

	return out
}
