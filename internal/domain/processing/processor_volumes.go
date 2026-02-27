package processing

import (
	"errors"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// buildPostCommitVolumes computes the post-commit volumes for all (account, asset)
// pairs involved in the given postings. It reads the current volume state from the
// in-memory store (after postings have been applied) and resolves Known/Diff pairs
// into concrete Input/Output values as big integer strings.
func buildPostCommitVolumes(s InMemoryStore, ledger string, postings []*commonpb.Posting) *commonpb.PostCommitVolumes {
	// Collect unique (account, asset) pairs
	type accountAsset struct {
		account string
		asset   string
	}
	seen := make(map[accountAsset]struct{})
	var pairs []accountAsset
	for _, p := range postings {
		srcKey := accountAsset{account: p.Source, asset: p.Asset}
		if _, ok := seen[srcKey]; !ok {
			seen[srcKey] = struct{}{}
			pairs = append(pairs, srcKey)
		}
		dstKey := accountAsset{account: p.Destination, asset: p.Asset}
		if _, ok := seen[dstKey]; !ok {
			seen[dstKey] = struct{}{}
			pairs = append(pairs, dstKey)
		}
	}

	volumesByAccount := make(map[string]*commonpb.VolumesByAssets, len(pairs))
	var scratch uint256.Int

	for _, pair := range pairs {
		vol, err := s.GetVolume(domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledger, Account: pair.account},
			Asset:      pair.asset,
		})
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			continue
		}

		// Resolve Input: Known takes priority, then Diff, then zero
		var inputStr, outputStr string
		if vol != nil {
			if vol.InputKnown != nil {
				vol.InputKnown.IntoUint256(&scratch)
				inputStr = scratch.Dec()
			} else if vol.InputDiff != nil {
				vol.InputDiff.IntoUint256(&scratch)
				inputStr = scratch.Dec()
			} else {
				inputStr = "0"
			}

			if vol.OutputKnown != nil {
				vol.OutputKnown.IntoUint256(&scratch)
				outputStr = scratch.Dec()
			} else if vol.OutputDiff != nil {
				vol.OutputDiff.IntoUint256(&scratch)
				outputStr = scratch.Dec()
			} else {
				outputStr = "0"
			}
		} else {
			inputStr = "0"
			outputStr = "0"
		}

		byAssets, ok := volumesByAccount[pair.account]
		if !ok {
			byAssets = &commonpb.VolumesByAssets{
				Volumes: make(map[string]*commonpb.Volumes),
			}
			volumesByAccount[pair.account] = byAssets
		}
		byAssets.Volumes[pair.asset] = &commonpb.Volumes{
			Input:  inputStr,
			Output: outputStr,
		}
	}

	return &commonpb.PostCommitVolumes{
		VolumesByAccount: volumesByAccount,
	}
}
