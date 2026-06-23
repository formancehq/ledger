package domain

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// TouchedVolumeSet builds an AccountAssetKey set from a list of
// commonpb.TouchedVolume entries. Shared by the index builder (which
// reads AppliedProposal.TransientVolumes and LedgerLog.PurgedVolumes
// off the projection records) and the integrity checker (which mirrors
// the same projections for its corruption-detection pass), so the
// "iterate, key, dedupe" boilerplate stays in one place.
//
// Returns nil for an empty input so callers can preserve their
// "nothing to exclude" fast path without an extra len check.
func TouchedVolumeSet(volumes []*commonpb.TouchedVolume) map[AccountAssetKey]struct{} {
	if len(volumes) == 0 {
		return nil
	}

	set := make(map[AccountAssetKey]struct{}, len(volumes))
	for _, v := range volumes {
		set[AccountAssetKey{Account: v.GetAccount(), Asset: v.GetAsset()}] = struct{}{}
	}

	return set
}
