package processing

import (
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// buildPostCommitVolumes computes the post-commit volumes for all
// (account, asset, color) tuples involved in the given postings. It reads the
// current volume state from the in-memory store (after postings have been
// applied) and converts Known values into concrete Input/Output values as big
// integer strings.
//
// The returned VolumesByAssets list is sorted by (asset, color) ascending so
// the response is deterministic across reads.
//
// Reads go through readVolumeOrZero: a declared-but-absent key (domain.ErrNotFound)
// is reported as a zero balance, while any other error — notably
// *state.ErrCoverageMiss, an admission-contract violation (invariants #6/#9) that
// is impossible by design under a correct preload — is propagated as an
// ErrStorageOperation so the order is rejected loudly (invariant #7) rather than
// returned to the client as a silently truncated volume map (EN-1440). Two
// nodes must not emit divergent PCV payloads for the same applied index, so a
// non-NotFound store error is always surfaced. This mirrors applyPosting, which
// reads the same source+destination keys.
func buildPostCommitVolumes(s Scope, ledgerName string, postings []*commonpb.Posting) (*commonpb.PostCommitVolumes, domain.Describable) {
	type tuple struct {
		account string
		asset   string
		color   string
	}

	seen := make(map[tuple]struct{})

	var tuples []tuple

	add := func(t tuple) {
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
		tuples = append(tuples, t)
	}

	for _, p := range postings {
		color := p.GetColor()
		add(tuple{account: p.GetSource(), asset: p.GetAsset(), color: color})
		add(tuple{account: p.GetDestination(), asset: p.GetAsset(), color: color})
	}

	volumesByAccount := make(map[string]*commonpb.VolumesByAssets, len(tuples))

	var scratch uint256.Int

	for _, t := range tuples {
		vol, err := readVolumeOrZero(s, domain.NewVolumeKey(ledgerName, t.account, t.asset, t.color))
		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "loading post-commit volume", Cause: err}
		}

		vol.GetInput().IntoUint256(&scratch)
		inputStr := scratch.Dec()
		vol.GetOutput().IntoUint256(&scratch)
		outputStr := scratch.Dec()

		byAssets, ok := volumesByAccount[t.account]
		if !ok {
			byAssets = &commonpb.VolumesByAssets{}
			volumesByAccount[t.account] = byAssets
		}
		byAssets.Volumes = append(byAssets.Volumes, &commonpb.VolumeEntry{
			Asset: t.asset,
			Color: t.color,
			Volumes: &commonpb.Volumes{
				Input:  inputStr,
				Output: outputStr,
			},
		})
	}

	out := &commonpb.PostCommitVolumes{VolumesByAccount: volumesByAccount}
	out.SortVolumes()

	return out, nil
}
