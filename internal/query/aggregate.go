package query

import (
	"fmt"
	"sort"
	"strings"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// assetKey identifies an asset by its base name and precision, used as map key
// in the aggregator to avoid string formatting/parsing round-trips.
type assetKey struct {
	base      string
	precision uint8
}

// aggregatedVol accumulates input/output volumes for a single asset.
type aggregatedVol struct {
	input  *uint256.Int
	output *uint256.Int
}

// volumeAggregator collects volume entries and builds the final result.
type volumeAggregator struct {
	byAsset         map[assetKey]*aggregatedVol
	useMaxPrecision bool
}

func newVolumeAggregator(useMaxPrecision bool) *volumeAggregator {
	return &volumeAggregator{
		byAsset:         make(map[assetKey]*aggregatedVol),
		useMaxPrecision: useMaxPrecision,
	}
}

func (va *volumeAggregator) accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
	var vk domain.VolumeKey

	err := vk.Unmarshal(entry.CanonicalKey)
	if err != nil {
		return fmt.Errorf("unmarshaling volume key: %w", err)
	}

	va.accumulateAsset(vk.AssetBase, vk.AssetPrecision, entry.Value)

	return nil
}

func (va *volumeAggregator) accumulateAsset(base string, precision uint8, value *raftcmdpb.VolumePair) {
	key := assetKey{base: base, precision: precision}

	agg, ok := va.byAsset[key]
	if !ok {
		agg = &aggregatedVol{
			input:  new(uint256.Int),
			output: new(uint256.Int),
		}
		va.byAsset[key] = agg
	}

	if value != nil {
		var tmp uint256.Int
		if value.GetInput() != nil {
			value.GetInput().IntoUint256(&tmp)
			agg.input.Add(agg.input, &tmp)
		}

		if value.GetOutput() != nil {
			value.GetOutput().IntoUint256(&tmp)
			agg.output.Add(agg.output, &tmp)
		}
	}
}

// pow10 returns 10^exp as a uint256.Int.
func pow10(exp uint8) *uint256.Int {
	result := uint256.NewInt(1)
	ten := uint256.NewInt(10)

	for range exp {
		result.Mul(result, ten)
	}

	return result
}

func (va *volumeAggregator) result() *commonpb.AggregateResult {
	if va.useMaxPrecision {
		return va.resultWithMaxPrecision()
	}

	volumes := make([]*commonpb.AggregatedVolume, 0, len(va.byAsset))
	for key, agg := range va.byAsset {
		volumes = append(volumes, &commonpb.AggregatedVolume{
			Asset:  domain.FormatAsset(key.base, key.precision),
			Input:  commonpb.NewUint256(agg.input),
			Output: commonpb.NewUint256(agg.output),
		})
	}

	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].GetAsset() < volumes[j].GetAsset()
	})

	return &commonpb.AggregateResult{Volumes: volumes}
}

// resultWithMaxPrecision merges assets sharing the same base under the highest
// precision observed, rescaling lower-precision amounts.
func (va *volumeAggregator) resultWithMaxPrecision() *commonpb.AggregateResult {
	// First pass: find max precision per asset base.
	maxPrec := make(map[string]uint8)
	for key := range va.byAsset {
		if key.precision > maxPrec[key.base] {
			maxPrec[key.base] = key.precision
		}
	}

	// Second pass: rescale and merge under target precision.
	merged := make(map[assetKey]*aggregatedVol)

	for key, agg := range va.byAsset {
		target := maxPrec[key.base]
		mergedKey := assetKey{base: key.base, precision: target}

		m, ok := merged[mergedKey]
		if !ok {
			m = &aggregatedVol{
				input:  new(uint256.Int),
				output: new(uint256.Int),
			}
			merged[mergedKey] = m
		}

		if key.precision == target {
			m.input.Add(m.input, agg.input)
			m.output.Add(m.output, agg.output)
		} else {
			factor := pow10(target - key.precision)

			var scaled uint256.Int
			scaled.Mul(agg.input, factor)
			m.input.Add(m.input, &scaled)

			scaled.Mul(agg.output, factor)
			m.output.Add(m.output, &scaled)
		}
	}

	volumes := make([]*commonpb.AggregatedVolume, 0, len(merged))
	for key, agg := range merged {
		volumes = append(volumes, &commonpb.AggregatedVolume{
			Asset:  domain.FormatAsset(key.base, key.precision),
			Input:  commonpb.NewUint256(agg.input),
			Output: commonpb.NewUint256(agg.output),
		})
	}

	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].GetAsset() < volumes[j].GetAsset()
	})

	return &commonpb.AggregateResult{Volumes: volumes}
}

// AggregateOptions configures volume aggregation behavior.
type AggregateOptions struct {
	UseMaxPrecision bool
	GroupByPrefixes []string
}

// groupedAggregator dispatches volume entries to per-prefix volumeAggregators.
// Each account is assigned to the first matching prefix.
type groupedAggregator struct {
	prefixes    []string
	aggregators map[string]*volumeAggregator
}

func newGroupedAggregator(opts AggregateOptions) *groupedAggregator {
	aggs := make(map[string]*volumeAggregator, len(opts.GroupByPrefixes))
	for _, p := range opts.GroupByPrefixes {
		aggs[p] = newVolumeAggregator(opts.UseMaxPrecision)
	}

	return &groupedAggregator{
		prefixes:    opts.GroupByPrefixes,
		aggregators: aggs,
	}
}

func (ga *groupedAggregator) matchPrefix(account string) (string, bool) {
	for _, p := range ga.prefixes {
		if strings.HasPrefix(account, p) {
			return p, true
		}
	}

	return "", false
}

func (ga *groupedAggregator) accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
	var vk domain.VolumeKey
	if err := vk.Unmarshal(entry.CanonicalKey); err != nil {
		return fmt.Errorf("unmarshaling volume key: %w", err)
	}

	prefix, ok := ga.matchPrefix(vk.Account)
	if !ok {
		return nil // account doesn't match any prefix, skip
	}

	ga.aggregators[prefix].accumulateAsset(vk.AssetBase, vk.AssetPrecision, entry.Value)

	return nil
}

func (ga *groupedAggregator) result() *commonpb.AggregateResult {
	groups := make([]*commonpb.GroupedAggregateResult, 0, len(ga.prefixes))
	for _, p := range ga.prefixes {
		groups = append(groups, &commonpb.GroupedAggregateResult{
			Prefix:  p,
			Volumes: ga.aggregators[p].result().GetVolumes(),
		})
	}

	return &commonpb.AggregateResult{Groups: groups}
}

// AggregateVolumes executes a cross-store merge-scan for filtered aggregation:
// 1. Iterate matching accounts from Pebble (accountIter)
// 2. For each account, scan volumes in Pebble via StreamingIter
// 3. Accumulate per-asset totals.
func AggregateVolumes(
	pebbleReader dal.PebbleReader,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledger string,
	accountIter readstore.EntityIterator,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	acc := newAccumulator(opts)

	for accountIter.Next() {
		account := string(accountIter.Current())

		// Build canonical prefix: [ledger]\x00[account]\x00
		// This matches all volume keys for this (ledger, account) pair.
		canonicalPrefix := make([]byte, len(ledger)+1+len(account)+1)
		n := copy(canonicalPrefix, ledger)
		canonicalPrefix[n] = 0x00
		n++
		n += copy(canonicalPrefix[n:], account)
		canonicalPrefix[n] = dal.CanonicalKeySepVolume

		iter, err := volumeAttr.NewStreamingIter(pebbleReader, canonicalPrefix)
		if err != nil {
			return nil, fmt.Errorf("creating volume iterator for account %q: %w", account, err)
		}

		for iter.Next() {
			if err := acc.accumulate(iter.Entry()); err != nil {
				_ = iter.Close()

				return nil, fmt.Errorf("accumulating volumes for account %q: %w", account, err)
			}
		}

		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("closing volume iterator for account %q: %w", account, err)
		}

		if err := iter.Err(); err != nil {
			return nil, fmt.Errorf("scanning volumes for account %q: %w", account, err)
		}
	}

	return acc.result(), nil
}

// AggregateAllVolumes performs unfiltered volume aggregation in a single Pebble
// scan. Instead of enumerating accounts then scanning volumes per account (N+1
// iterators, N SeekGE hops), it calls StreamingIter once with the ledger
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
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	acc := newAccumulator(opts)

	// Single-pass: scan [ledger\x00] prefix which covers all accounts.
	// StreamingIter on volumeAttr skips non-volume entries (metadata).
	ledgerPrefix := make([]byte, len(ledger)+1)
	copy(ledgerPrefix, ledger)
	ledgerPrefix[len(ledger)] = 0x00

	iter, err := volumeAttr.NewStreamingIter(pebbleReader, ledgerPrefix)
	if err != nil {
		return nil, fmt.Errorf("creating volume iterator: %w", err)
	}

	for iter.Next() {
		if err := acc.accumulate(iter.Entry()); err != nil {
			_ = iter.Close()

			return nil, fmt.Errorf("accumulating volumes: %w", err)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("closing volume iterator: %w", err)
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scanning all volumes: %w", err)
	}

	return acc.result(), nil
}

// accumulator is the common interface for flat and grouped aggregation.
type accumulator interface {
	accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error
	result() *commonpb.AggregateResult
}

func newAccumulator(opts AggregateOptions) accumulator {
	if len(opts.GroupByPrefixes) > 0 {
		return newGroupedAggregator(opts)
	}

	return newVolumeAggregator(opts.UseMaxPrecision)
}
