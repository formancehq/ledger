package processing

import (
	"errors"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// buildPostCommitVolumes computes the post-commit volumes for all (account, asset)
// pairs involved in the given postings. It reads the current volume state from the
// in-memory store (after postings have been applied) and converts Known values
// into concrete Input/Output values as big integer strings.
func buildPostCommitVolumes(s InMemoryStore, ledgerID uint32, postings []*commonpb.Posting) *commonpb.PostCommitVolumes {
	// Collect unique (account, asset) pairs
	type accountAsset struct {
		account string
		asset   string
	}

	seen := make(map[accountAsset]struct{})

	var pairs []accountAsset

	for _, p := range postings {
		srcKey := accountAsset{account: p.GetSource(), asset: p.GetAsset()}
		if _, ok := seen[srcKey]; !ok {
			seen[srcKey] = struct{}{}
			pairs = append(pairs, srcKey)
		}

		dstKey := accountAsset{account: p.GetDestination(), asset: p.GetAsset()}
		if _, ok := seen[dstKey]; !ok {
			seen[dstKey] = struct{}{}
			pairs = append(pairs, dstKey)
		}
	}

	volumesByAccount := make(map[string]*commonpb.VolumesByAssets, len(pairs))

	var scratch uint256.Int

	for _, pair := range pairs {
		vol, err := s.GetVolume(domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: pair.account},
			Asset:      pair.asset,
		})
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			continue
		}

		var inputStr, outputStr string

		if vol != nil {
			vol.GetInput().IntoUint256(&scratch)
			inputStr = scratch.Dec()
			vol.GetOutput().IntoUint256(&scratch)
			outputStr = scratch.Dec()
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
