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

// isNewVolumeUpdate reports whether a volume update represents a
// first-time write to that (account, asset) key. "New" is defined by the
// preloaded prior value: absent or the zero placeholder → new; a defined
// non-zero prior value → pre-existing.
func isNewVolumeUpdate(u attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) bool {
	if !u.Old.IsDefined() {
		return true
	}

	return isVolumePreloadZero(u.Old.Value())
}

// volumeSetKey is the (ledger, account, asset) tuple used by the per-log
// intersection helpers below. Mirrors purgedVolumeKey's asset-dimension
// rationale — a multi-asset account may split across categories.
type volumeSetKey struct {
	Ledger  string
	Account string
	Asset   string
}

// makeNewKeptKeySet builds the set of (ledger, account, asset) tuples that
// were newly created AND survived past commit — i.e. persistent-new volumes
// that are NOT ephemeral. Consumed by buildNewKeptByLog.
func makeNewKeptKeySet(kept []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) map[volumeSetKey]struct{} {
	set := make(map[volumeSetKey]struct{})

	for i := range kept {
		if !isNewVolumeUpdate(kept[i]) {
			continue
		}

		set[volumeSetKey{
			Ledger:  kept[i].Key.LedgerName,
			Account: kept[i].Key.Account,
			Asset:   kept[i].Key.Asset,
		}] = struct{}{}
	}

	return set
}

// splitPurged partitions partResult.purged into pure-ephemeral (was zero,
// briefly touched, is zero at commit) and draining (was non-zero, back to
// zero). The two sets are disjoint by definition — a purged update either
// had a prior balance or did not.
func splitPurged(purged []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) (ephemeral, draining map[volumeSetKey]struct{}) {
	ephemeral = make(map[volumeSetKey]struct{})
	draining = make(map[volumeSetKey]struct{})

	for i := range purged {
		key := volumeSetKey{
			Ledger:  purged[i].Key.LedgerName,
			Account: purged[i].Key.Account,
			Asset:   purged[i].Key.Asset,
		}

		if isNewVolumeUpdate(purged[i]) {
			ephemeral[key] = struct{}{}
		} else {
			draining[key] = struct{}{}
		}
	}

	return ephemeral, draining
}

// buildTouchedByLog produces, for each order index, the deduplicated list of
// (account, asset) tuples the order touched that fall in the given set.
// Indexed by order_index; entries for orders with no matching keys are nil.
// Tuples within an entry are sorted (by account then asset) so the log
// payload is deterministic across nodes and runs.
//
// This is the generalisation of buildPurgedByLog / buildNewByLog into one
// helper — the caller supplies the intersection set (draining, ephemeral,
// or new-kept).
func buildTouchedByLog(perOrderVolumeKeys [][]domain.VolumeKey, set map[volumeSetKey]struct{}) [][]*commonpb.TouchedVolume {
	if len(perOrderVolumeKeys) == 0 || len(set) == 0 {
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
			if _, ok := set[volumeSetKey{Ledger: k.LedgerName, Account: k.Account, Asset: k.Asset}]; !ok {
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
