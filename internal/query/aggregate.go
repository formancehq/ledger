package query

import (
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// aggregatedVol accumulates input/output volumes for a single asset.
type aggregatedVol struct {
	input  *uint256.Int
	output *uint256.Int
}

// AggregateVolumes executes a cross-store merge-scan:
// 1. Iterate matching accounts from bbolt (accountIter)
// 2. For each account, scan volumes in Pebble via ForEachInPrefix
// 3. Accumulate per-asset totals.
func AggregateVolumes(
	pebbleReader dal.PebbleReader,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledger string,
	accountIter readstore.EntityIterator,
) (*commonpb.AggregateResult, error) {
	aggregator := make(map[string]*aggregatedVol)

	for accountIter.Next() {
		account := string(accountIter.Current())

		// Build canonical prefix: [ledger]\x00[account]\x00
		// This matches all volume keys for this (ledger, account) pair.
		canonicalPrefix := make([]byte, len(ledger)+1+len(account)+1)
		n := copy(canonicalPrefix, ledger)
		canonicalPrefix[n] = 0x00
		n++
		n += copy(canonicalPrefix[n:], account)
		canonicalPrefix[n] = 0x00

		err := volumeAttr.ForEachInPrefix(pebbleReader, ^uint64(0), canonicalPrefix, func(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
			var vk domain.VolumeKey

			err := vk.Unmarshal(entry.CanonicalKey)
			if err != nil {
				return fmt.Errorf("unmarshaling volume key: %w", err)
			}

			agg, ok := aggregator[vk.Asset]
			if !ok {
				agg = &aggregatedVol{
					input:  new(uint256.Int),
					output: new(uint256.Int),
				}
				aggregator[vk.Asset] = agg
			}

			if entry.Value != nil {
				var tmp uint256.Int
				if entry.Value.GetInput() != nil {
					entry.Value.GetInput().IntoUint256(&tmp)
					agg.input.Add(agg.input, &tmp)
				}

				if entry.Value.GetOutput() != nil {
					entry.Value.GetOutput().IntoUint256(&tmp)
					agg.output.Add(agg.output, &tmp)
				}
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scanning volumes for account %q: %w", account, err)
		}
	}

	// Build sorted result
	volumes := make([]*commonpb.AggregatedVolume, 0, len(aggregator))
	for asset, agg := range aggregator {
		volumes = append(volumes, &commonpb.AggregatedVolume{
			Asset:  asset,
			Input:  commonpb.NewUint256(agg.input),
			Output: commonpb.NewUint256(agg.output),
		})
	}

	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].GetAsset() < volumes[j].GetAsset()
	})

	return &commonpb.AggregateResult{Volumes: volumes}, nil
}
