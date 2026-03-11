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

// volumeAggregator collects volume entries and builds the final result.
type volumeAggregator struct {
	byAsset map[string]*aggregatedVol
}

func newVolumeAggregator() *volumeAggregator {
	return &volumeAggregator{byAsset: make(map[string]*aggregatedVol)}
}

func (va *volumeAggregator) accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
	var vk domain.VolumeKey

	err := vk.Unmarshal(entry.CanonicalKey)
	if err != nil {
		return fmt.Errorf("unmarshaling volume key: %w", err)
	}

	agg, ok := va.byAsset[vk.Asset]
	if !ok {
		agg = &aggregatedVol{
			input:  new(uint256.Int),
			output: new(uint256.Int),
		}
		va.byAsset[vk.Asset] = agg
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
}

func (va *volumeAggregator) result() *commonpb.AggregateResult {
	volumes := make([]*commonpb.AggregatedVolume, 0, len(va.byAsset))
	for asset, agg := range va.byAsset {
		volumes = append(volumes, &commonpb.AggregatedVolume{
			Asset:  asset,
			Input:  commonpb.NewUint256(agg.input),
			Output: commonpb.NewUint256(agg.output),
		})
	}

	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].GetAsset() < volumes[j].GetAsset()
	})

	return &commonpb.AggregateResult{Volumes: volumes}
}

// AggregateVolumes executes a cross-store merge-scan for filtered aggregation:
// 1. Iterate matching accounts from Pebble (accountIter)
// 2. For each account, scan volumes in Pebble via ForEachInPrefix
// 3. Accumulate per-asset totals.
func AggregateVolumes(
	pebbleReader dal.PebbleReader,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledger string,
	accountIter readstore.EntityIterator,
) (*commonpb.AggregateResult, error) {
	va := newVolumeAggregator()

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

		err := volumeAttr.ForEachInPrefix(pebbleReader, canonicalPrefix, va.accumulate)
		if err != nil {
			return nil, fmt.Errorf("scanning volumes for account %q: %w", account, err)
		}
	}

	return va.result(), nil
}

// AggregateAllVolumes performs unfiltered volume aggregation in a single Pebble
// scan. Instead of enumerating accounts then scanning volumes per account (N+1
// iterators, N SeekGE hops), it calls ForEachInPrefix once with the ledger
// prefix, yielding all volume entries in a single sequential pass.
//
// This is significantly faster for unfiltered aggregation because:
//   - 1 Pebble iterator instead of N+1
//   - sequential scan instead of N SeekGE hops
//   - no double-read of the same Pebble blocks
func AggregateAllVolumes(
	pebbleReader dal.PebbleReader,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledger string,
) (*commonpb.AggregateResult, error) {
	va := newVolumeAggregator()

	// Single-pass: scan [ledger\x00] prefix which covers all accounts.
	// ForEachInPrefix on volumeAttr skips non-volume entries (metadata).
	ledgerPrefix := make([]byte, len(ledger)+1)
	copy(ledgerPrefix, ledger)
	ledgerPrefix[len(ledger)] = 0x00

	err := volumeAttr.ForEachInPrefix(pebbleReader, ledgerPrefix, va.accumulate)
	if err != nil {
		return nil, fmt.Errorf("scanning all volumes: %w", err)
	}

	return va.result(), nil
}
