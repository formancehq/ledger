package state

import (
	"sort"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// isNewVolumeUpdate reports whether a volume update represents a first-time
// persistent write. Absent and deliberately purged cache cells are tombstones,
// so a defined Old always represents a persisted row, even when it is {0, 0}.
func isNewVolumeUpdate(u attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) bool {
	return !u.Old.IsDefined()
}

// volumeSetKey is the (ledger, account, asset, color) tuple used by the per-log
// intersection helpers below. Both asset and color dimensions are kept: a
// multi-bucket account may split across categories per (asset, color) — one
// (asset, color) bucket may be purged/new while another stays kept, so dropping
// either dimension would over-attribute a category to orders touching a
// still-kept bucket.
type volumeSetKey struct {
	Ledger  string
	Account string
	Asset   string
	Color   string
}

// makeNewKeptKeySet builds the set of (ledger, account, asset, color) tuples
// that were newly created AND survived past commit — persistent-new volumes.
// Consumed by buildTouchedByLog.
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
			Color:   kept[i].Key.Color,
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
			Color:   purged[i].Key.Color,
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
// (account, asset, color) tuples the order touched that fall in the given set.
// Indexed by order_index; entries for orders with no matching keys are nil.
// Tuples within an entry are sorted (by account, asset, then color) so the log
// payload is deterministic across nodes and runs.
//
// This is the generalisation of buildPurgedByLog / buildNewByLog into one
// helper — the caller supplies the intersection set (draining, ephemeral,
// or new-kept). The color dimension is preserved so multi-bucket accounts are
// attributed per (asset, color) bucket rather than collapsed onto the asset.
func buildTouchedByLog(perOrderVolumeKeys [][]domain.VolumeKey, set map[volumeSetKey]struct{}) [][]*ledgerpb.TouchedVolume {
	if len(perOrderVolumeKeys) == 0 || len(set) == 0 {
		return nil
	}

	type accAssetColor struct{ Account, Asset, Color string }

	out := make([][]*ledgerpb.TouchedVolume, len(perOrderVolumeKeys))
	for i, keys := range perOrderVolumeKeys {
		if len(keys) == 0 {
			continue
		}

		seen := make(map[accAssetColor]struct{}, len(keys))
		for _, k := range keys {
			if _, ok := set[volumeSetKey{Ledger: k.LedgerName, Account: k.Account, Asset: k.Asset, Color: k.Color}]; !ok {
				continue
			}
			seen[accAssetColor{Account: k.Account, Asset: k.Asset, Color: k.Color}] = struct{}{}
		}

		if len(seen) == 0 {
			continue
		}

		ordered := make([]accAssetColor, 0, len(seen))
		for k := range seen {
			ordered = append(ordered, k)
		}
		sort.Slice(ordered, func(a, b int) bool {
			if ordered[a].Account != ordered[b].Account {
				return ordered[a].Account < ordered[b].Account
			}
			if ordered[a].Asset != ordered[b].Asset {
				return ordered[a].Asset < ordered[b].Asset
			}

			return ordered[a].Color < ordered[b].Color
		})

		vols := make([]*ledgerpb.TouchedVolume, len(ordered))
		for j, k := range ordered {
			vols[j] = &ledgerpb.TouchedVolume{Account: k.Account, Asset: k.Asset, Color: k.Color}
		}
		out[i] = vols
	}

	return out
}
