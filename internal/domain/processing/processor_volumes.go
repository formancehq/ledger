package processing

import (
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// buildPostCommitVolumes computes the post-commit volumes for all (account, asset)
// pairs involved in the given postings. It reads the current volume state from the
// in-memory store (after postings have been applied) and converts Known values
// into concrete Input/Output values as big integer strings.
//
// Reads go through readVolumeOrZero: a declared-but-absent key (domain.ErrNotFound)
// is reported as a zero balance, while any other error — notably
// *state.ErrCoverageMiss, an admission-contract violation (invariants #6/#9) that
// is impossible by design under a correct preload — is propagated as an
// ErrStorageOperation so the order is rejected loudly (invariant #7) rather than
// returned to the client as a silently truncated volume map (EN-1440). This
// mirrors applyPosting, which reads the same source+destination keys.
func buildPostCommitVolumes(s Scope, ledgerName string, postings []*commonpb.Posting) (*commonpb.PostCommitVolumes, domain.Describable) {
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
		vol, err := readVolumeOrZero(s, domain.NewVolumeKey(ledgerName, pair.account, pair.asset))
		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "loading post-commit volume", Cause: err}
		}

		vol.GetInput().IntoUint256(&scratch)
		inputStr := scratch.Dec()
		vol.GetOutput().IntoUint256(&scratch)
		outputStr := scratch.Dec()

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
	}, nil
}
