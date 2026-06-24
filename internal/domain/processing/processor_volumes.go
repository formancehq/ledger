package processing

import (
	"errors"

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
// A non-NotFound storage error from GetVolume is surfaced through the
// returned error (rather than silently skipping the tuple): two nodes with
// different transient store errors would otherwise emit divergent PCV
// payloads for the same applied index, breaking the determinism invariant.
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
		vol, err := s.Volumes().Get(domain.NewVolumeKey(ledgerName, t.account, t.asset, t.color))
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrStorageOperation{
				Operation: "buildPostCommitVolumes: loading volume",
				Cause:     err,
			}
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
