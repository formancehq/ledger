package query

import (
	"fmt"
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

// aggregatedVol accumulates input/output volumes for a single bucket.
type aggregatedVol struct {
	input  *uint256.Int
	output *uint256.Int
}

// volumeAggregator collects volume entries and builds the final result.
type volumeAggregator struct {
	byAsset         map[assetKey]*aggregatedVol
	useMaxPrecision bool
	collapseColors  bool
}

func newVolumeAggregator(useMaxPrecision, collapseColors bool) *volumeAggregator {
	return &volumeAggregator{
		byAsset:         make(map[assetKey]*aggregatedVol),
		useMaxPrecision: useMaxPrecision,
		collapseColors:  collapseColors,
	}
}

func (va *volumeAggregator) accumulate(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
	var vk domain.VolumeKey

	err := vk.Unmarshal(entry.CanonicalKey)
	if err != nil {
		return fmt.Errorf("unmarshaling volume key: %w", err)
	}

	return va.accumulateAsset(vk.AssetBase, vk.AssetPrecision, vk.Color, entry.Value)
}

func (va *volumeAggregator) accumulateAsset(base string, precision uint8, color string, value *raftcmdpb.VolumePair) error {
	key := assetKey{base: base, precision: precision, color: color}

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
			if _, overflow := agg.input.AddOverflow(agg.input, &tmp); overflow {
				return &ErrAggregateOverflow{Stage: "accumulate", Side: "input"}
			}
		}

		if value.GetOutput() != nil {
			value.GetOutput().IntoUint256(&tmp)
			if _, overflow := agg.output.AddOverflow(agg.output, &tmp); overflow {
				return &ErrAggregateOverflow{Stage: "accumulate", Side: "output"}
			}
		}
	}

	return nil
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

func (va *volumeAggregator) result() (*commonpb.AggregateResult, error) {
	if va.useMaxPrecision {
		return va.resultWithMaxPrecision()
	}

	buckets := va.byAsset
	if va.collapseColors {
		var err error
		buckets, err = collapseColorBuckets(buckets)
		if err != nil {
			return nil, err
		}
	}

	volumes := make([]*commonpb.AggregatedVolume, 0, len(buckets))
	for key, agg := range buckets {
		volumes = append(volumes, &commonpb.AggregatedVolume{
			Asset:  domain.FormatAsset(key.base, key.precision),
			Color:  key.color,
			Input:  commonpb.NewUint256(agg.input),
			Output: commonpb.NewUint256(agg.output),
		})
	}

	sortAggregatedVolumes(volumes)

	return &commonpb.AggregateResult{Volumes: volumes}, nil
}

// collapseColorBuckets sums all color buckets of the same (base, precision)
// into the empty-color bucket, returning a new map. Used when the caller
// explicitly requests collapse_colors. Overflow on the cross-color sum
// surfaces ErrAggregateOverflow rather than silently wrapping (the FSM
// already rejects per-bucket overflow on write, #321).
func collapseColorBuckets(in map[assetKey]*aggregatedVol) (map[assetKey]*aggregatedVol, error) {
	out := make(map[assetKey]*aggregatedVol, len(in))
	for key, agg := range in {
		k := assetKey{base: key.base, precision: key.precision}
		merged, ok := out[k]
		if !ok {
			merged = &aggregatedVol{
				input:  new(uint256.Int),
				output: new(uint256.Int),
			}
			out[k] = merged
		}
		if _, overflow := merged.input.AddOverflow(merged.input, agg.input); overflow {
			return nil, &ErrAggregateOverflow{Stage: "collapse-colors", Side: "input"}
		}
		if _, overflow := merged.output.AddOverflow(merged.output, agg.output); overflow {
			return nil, &ErrAggregateOverflow{Stage: "collapse-colors", Side: "output"}
		}
	}

	return out, nil
}

func sortAggregatedVolumes(volumes []*commonpb.AggregatedVolume) {
	sort.Slice(volumes, func(i, j int) bool {
		return commonpb.LessByAssetColor(volumes[i], volumes[j])
	})
}

// resultWithMaxPrecision merges assets sharing the same base under the highest
// precision observed, rescaling lower-precision amounts. Color is preserved as
// part of the bucket key (and optionally collapsed afterwards).
func (va *volumeAggregator) resultWithMaxPrecision() (*commonpb.AggregateResult, error) {
	// First pass: find max precision per asset base.
	maxPrec := make(map[string]uint8)
	for key := range va.byAsset {
		if key.precision > maxPrec[key.base] {
			maxPrec[key.base] = key.precision
		}
	}

	// Second pass: rescale and merge under target precision, keeping color.
	merged := make(map[assetKey]*aggregatedVol)

	for key, agg := range va.byAsset {
		target := maxPrec[key.base]
		mergedKey := assetKey{base: key.base, precision: target, color: key.color}

		m, ok := merged[mergedKey]
		if !ok {
			m = &aggregatedVol{
				input:  new(uint256.Int),
				output: new(uint256.Int),
			}
			merged[mergedKey] = m
		}

		if key.precision == target {
			if _, overflow := m.input.AddOverflow(m.input, agg.input); overflow {
				return nil, &ErrAggregateOverflow{Stage: "max-precision-merge", Side: "input"}
			}
			if _, overflow := m.output.AddOverflow(m.output, agg.output); overflow {
				return nil, &ErrAggregateOverflow{Stage: "max-precision-merge", Side: "output"}
			}
		} else {
			factor := pow10(target - key.precision)

			var scaled uint256.Int
			if _, overflow := scaled.MulOverflow(agg.input, factor); overflow {
				return nil, &ErrAggregateOverflow{Stage: "max-precision-rescale", Side: "input"}
			}
			if _, overflow := m.input.AddOverflow(m.input, &scaled); overflow {
				return nil, &ErrAggregateOverflow{Stage: "max-precision-rescale", Side: "input"}
			}

			if _, overflow := scaled.MulOverflow(agg.output, factor); overflow {
				return nil, &ErrAggregateOverflow{Stage: "max-precision-rescale", Side: "output"}
			}
			if _, overflow := m.output.AddOverflow(m.output, &scaled); overflow {
				return nil, &ErrAggregateOverflow{Stage: "max-precision-rescale", Side: "output"}
			}
		}
	}

	if va.collapseColors {
		var err error
		merged, err = collapseColorBuckets(merged)
		if err != nil {
			return nil, err
		}
	}

	volumes := make([]*commonpb.AggregatedVolume, 0, len(merged))
	for key, agg := range merged {
		volumes = append(volumes, &commonpb.AggregatedVolume{
			Asset:  domain.FormatAsset(key.base, key.precision),
			Color:  key.color,
			Input:  commonpb.NewUint256(agg.input),
			Output: commonpb.NewUint256(agg.output),
		})
	}

	sortAggregatedVolumes(volumes)

	return &commonpb.AggregateResult{Volumes: volumes}, nil
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

// groupedAggregator dispatches volume entries to per-prefix volumeAggregators.
// Each account is assigned to the first matching prefix.
type groupedAggregator struct {
	prefixes    []string
	aggregators map[string]*volumeAggregator
}

func newGroupedAggregator(opts AggregateOptions) *groupedAggregator {
	aggs := make(map[string]*volumeAggregator, len(opts.GroupByPrefixes))
	for _, p := range opts.GroupByPrefixes {
		aggs[p] = newVolumeAggregator(opts.UseMaxPrecision, opts.CollapseColors)
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

	return ga.aggregators[prefix].accumulateAsset(vk.AssetBase, vk.AssetPrecision, vk.Color, entry.Value)
}

func (ga *groupedAggregator) result() (*commonpb.AggregateResult, error) {
	groups := make([]*commonpb.GroupedAggregateResult, 0, len(ga.prefixes))
	for _, p := range ga.prefixes {
		res, err := ga.aggregators[p].result()
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

	return acc.result()
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

	return newVolumeAggregator(opts.UseMaxPrecision, opts.CollapseColors)
}
