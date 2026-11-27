package query

import (
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// assetKey identifies a balance bucket by (base, precision, color). Color is
// always part of the key — different colors are distinct buckets. To collapse
// across colors at the result stage, use AggregateOptions.CollapseColors.
type assetKey struct {
	base      string
	precision uint8
	color     string
}

// wideSum is an exact running total: wraps counts how many times low
// overflowed 2^256, so the value is wraps·2^256 + low.
type wideSum struct {
	low   uint256.Int
	wraps uint64
}

func (s *wideSum) add(v *uint256.Int) {
	if _, overflow := s.low.AddOverflow(&s.low, v); overflow {
		s.wraps++
	}
}

func (s *wideSum) toBig() *big.Int {
	total := new(big.Int).SetUint64(s.wraps)
	total.Lsh(total, 256)

	return total.Add(total, s.low.ToBig())
}

// aggregatedVol accumulates input/output volumes for a single bucket.
type aggregatedVol struct {
	input  wideSum
	output wideSum
}

type exactVol struct {
	input  *big.Int
	output *big.Int
}

func newExactVol() *exactVol {
	return &exactVol{input: new(big.Int), output: new(big.Int)}
}

func (v *exactVol) add(other *exactVol) {
	v.input.Add(v.input, other.input)
	v.output.Add(v.output, other.output)
}

// volumeAggregator collects volume entries and builds the final result.
type volumeAggregator struct {
	byAsset         map[assetKey]*aggregatedVol
	useMaxPrecision bool
	collapseColors  bool
}

func newVolumeAggregator(opts AggregateOptions) *volumeAggregator {
	return &volumeAggregator{
		byAsset:         make(map[assetKey]*aggregatedVol),
		useMaxPrecision: opts.UseMaxPrecision,
		collapseColors:  opts.CollapseColors,
	}
}

func (va *volumeAggregator) accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
	var vk domain.VolumeKey

	err := vk.Unmarshal(entry.CanonicalKey)
	if err != nil {
		return fmt.Errorf("unmarshaling volume key: %w", err)
	}

	va.accumulateAsset(vk.AssetBase, vk.AssetPrecision, vk.Color, entry.Value)

	return nil
}

func (va *volumeAggregator) accumulateAsset(base string, precision uint8, color string, value *raftcmdpb.VolumePair) {
	key := assetKey{base: base, precision: precision, color: color}

	agg, ok := va.byAsset[key]
	if !ok {
		agg = &aggregatedVol{}
		va.byAsset[key] = agg
	}

	if value == nil {
		return
	}

	var tmp uint256.Int
	if value.GetInput() != nil {
		value.GetInput().IntoUint256(&tmp)
		agg.input.add(&tmp)
	}

	if value.GetOutput() != nil {
		value.GetOutput().IntoUint256(&tmp)
		agg.output.add(&tmp)
	}
}

func (va *volumeAggregator) result() (*commonpb.AggregateResult, error) {
	buckets := make(map[assetKey]*exactVol, len(va.byAsset))
	for key, agg := range va.byAsset {
		buckets[key] = &exactVol{input: agg.input.toBig(), output: agg.output.toBig()}
	}

	if va.useMaxPrecision {
		buckets = mergeAtMaxPrecision(buckets)
	}

	if va.collapseColors {
		buckets = collapseColorBuckets(buckets)
	}

	volumes := make([]*commonpb.AggregatedVolume, 0, len(buckets))
	for key, vol := range buckets {
		input, err := commonpb.NewBigUint(vol.input)
		if err != nil {
			return nil, fmt.Errorf("encoding aggregate input: %w", err)
		}
		output, err := commonpb.NewBigUint(vol.output)
		if err != nil {
			return nil, fmt.Errorf("encoding aggregate output: %w", err)
		}

		volumes = append(volumes, &commonpb.AggregatedVolume{
			Asset:  domain.FormatAsset(key.base, key.precision),
			Color:  key.color,
			Input:  input,
			Output: output,
		})
	}

	sort.Slice(volumes, func(i, j int) bool {
		return commonpb.LessByAssetColor(volumes[i], volumes[j])
	})

	return &commonpb.AggregateResult{Volumes: volumes}, nil
}

// collapseColorBuckets sums all color buckets of the same (base, precision)
// into the empty-color bucket, returning a new map. Used when the caller
// explicitly requests collapse_colors.
func collapseColorBuckets(in map[assetKey]*exactVol) map[assetKey]*exactVol {
	out := make(map[assetKey]*exactVol, len(in))
	for key, vol := range in {
		k := assetKey{base: key.base, precision: key.precision}
		merged, ok := out[k]
		if !ok {
			merged = newExactVol()
			out[k] = merged
		}
		merged.add(vol)
	}

	return out
}

// mergeAtMaxPrecision merges buckets sharing the same base under the highest
// precision observed, rescaling lower-precision amounts. Color is preserved as
// part of the bucket key.
func mergeAtMaxPrecision(in map[assetKey]*exactVol) map[assetKey]*exactVol {
	maxPrec := make(map[string]uint8)
	for key := range in {
		if key.precision > maxPrec[key.base] {
			maxPrec[key.base] = key.precision
		}
	}

	out := make(map[assetKey]*exactVol, len(in))
	for key, vol := range in {
		target := maxPrec[key.base]
		mergedKey := assetKey{base: key.base, precision: target, color: key.color}

		merged, ok := out[mergedKey]
		if !ok {
			merged = newExactVol()
			out[mergedKey] = merged
		}

		if key.precision == target {
			merged.add(vol)

			continue
		}

		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(target-key.precision)), nil)
		merged.add(&exactVol{
			input:  new(big.Int).Mul(vol.input, factor),
			output: new(big.Int).Mul(vol.output, factor),
		})
	}

	return out
}

// AggregateOptions configures volume aggregation behavior.
type AggregateOptions struct {
	UseMaxPrecision bool
	GroupByPrefixes []string
	// CollapseColors, when true, sums all color buckets of the same
	// (asset_base, precision) into the empty-color bucket in the result.
	// By default each (asset, color) tuple yields its own AggregatedVolume.
	CollapseColors bool
}

// groupedAggregator dispatches volume entries to one volumeAggregator per
// requested prefix entry. Each account is assigned to the first matching entry,
// so a repeated prefix yields an empty group.
type groupedAggregator struct {
	prefixes    []string
	aggregators []*volumeAggregator
}

func newGroupedAggregator(opts AggregateOptions) *groupedAggregator {
	aggs := make([]*volumeAggregator, len(opts.GroupByPrefixes))
	for i := range aggs {
		aggs[i] = newVolumeAggregator(opts)
	}

	return &groupedAggregator{
		prefixes:    opts.GroupByPrefixes,
		aggregators: aggs,
	}
}

func (ga *groupedAggregator) matchPrefix(account string) (int, bool) {
	for i, p := range ga.prefixes {
		if strings.HasPrefix(account, p) {
			return i, true
		}
	}

	return 0, false
}

func (ga *groupedAggregator) accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
	var vk domain.VolumeKey
	if err := vk.Unmarshal(entry.CanonicalKey); err != nil {
		return fmt.Errorf("unmarshaling volume key: %w", err)
	}

	i, ok := ga.matchPrefix(vk.Account)
	if !ok {
		return nil // account doesn't match any prefix, skip
	}

	ga.aggregators[i].accumulateAsset(vk.AssetBase, vk.AssetPrecision, vk.Color, entry.Value)

	return nil
}

func (ga *groupedAggregator) result() (*commonpb.AggregateResult, error) {
	groups := make([]*commonpb.GroupedAggregateResult, 0, len(ga.prefixes))
	for i, p := range ga.prefixes {
		res, err := ga.aggregators[i].result()
		if err != nil {
			return nil, err
		}
		groups = append(groups, &commonpb.GroupedAggregateResult{
			Prefix:  p,
			Volumes: res.GetVolumes(),
		})
	}

	return &commonpb.AggregateResult{Groups: groups}, nil
}

// AggregateVolumes executes a cross-store merge-scan for filtered aggregation:
// 1. Iterate matching accounts from Pebble (accountIter)
// 2. For each account, scan volumes in Pebble via StreamingIter
// 3. Accumulate per-asset totals.
func AggregateVolumes(
	pebbleReader dal.PebbleReader,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledgerName string,
	accountIter readstore.EntityIterator,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	acc := newAccumulator(opts)

	for accountIter.Next() {
		account := string(accountIter.Current())

		// Build canonical prefix: [ledgerName padded 64B][account][sepVolume].
		// This matches all volume keys for this (ledger, account) pair.
		canonicalPrefix := make([]byte, dal.LedgerNameFixedSize+len(account)+1)
		copy(canonicalPrefix[:dal.LedgerNameFixedSize], ledgerName)
		n := dal.LedgerNameFixedSize
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

	// The account iterator is a compiled filter tree whose leaves report a
	// failure in band: Next returns false for an error exactly as it does for
	// exhaustion. Unchecked, a fault partway through the accounts yields a
	// total summed over the ones scanned before it — an aggregate that is
	// wrong in a way nothing downstream can detect.
	if err := accountIter.Err(); err != nil {
		return nil, fmt.Errorf("scanning accounts: %w", err)
	}

	return acc.result()
}

// AggregateAllVolumes performs unfiltered volume aggregation in a single Pebble
// scan. Instead of enumerating accounts then scanning volumes per account (N+2
// physical iterators, N Seek hops), it calls StreamingIter once with the ledger
// prefix, yielding all volume entries in a single sequential pass.
//
// This is significantly faster for unfiltered aggregation because:
//   - 1 Pebble iterator instead of N+2 (volume and metadata account scans)
//   - sequential scan instead of N Seek hops
//   - no double-read of the same Pebble blocks
func AggregateAllVolumes(
	pebbleReader dal.PebbleReader,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledgerName string,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	acc := newAccumulator(opts)

	// Single-pass: scan [ledgerName padded 64B] prefix which covers all accounts.
	// StreamingIter on volumeAttr skips non-volume entries (metadata).
	ledgerPrefix := make([]byte, dal.LedgerNameFixedSize)
	copy(ledgerPrefix, ledgerName)

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

	return acc.result()
}

// accumulator is the common interface for flat and grouped aggregation.
type accumulator interface {
	accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error
	result() (*commonpb.AggregateResult, error)
}

func newAccumulator(opts AggregateOptions) accumulator {
	if len(opts.GroupByPrefixes) > 0 {
		return newGroupedAggregator(opts)
	}

	return newVolumeAggregator(opts)
}
