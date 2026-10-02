package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/holiman/uint256"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runAggregateQuery issues a linearizable AggregateVolumes over an index-free
// filter (or the whole ledger) and checks the per-bucket sums against the
// model's candidate bases. A bucket is one (asset, color); the request may
// collapse colors into the uncolored bucket and merge an asset's precisions
// under the highest one seen, and the model folds the same way. Grouping by
// prefix stays off.
func runAggregateQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 2)

	var filter *commonpb.QueryFilter
	switch {
	case oneIn(3):
		filter = genAccountFilterIndexed(c.sampleAccountFieldSeeds(ledger), 0)
	case oneIn(2):
		filter = genAccountFilterFree(0)
	}

	needed := map[string]struct{}{}
	neededIndexCanonicals(filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, needed)

	opts := aggOptions{
		collapseColors:  oneIn(3),
		useMaxPrecision: oneIn(4),
		groupByPrefixes: genGroupPrefixes(),
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// A linearizable read is served at a fixed Raft horizon at or past every
	// bulk whose response this driver has observed (EN-1946), so the fold stays
	// representable by a candidate base.
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	res, err := client.AggregateVolumes(readCtx, &servicepb.AggregateVolumesRequest{
		Ledger:          ledger,
		Filter:          filter,
		CollapseColors:  opts.collapseColors,
		UseMaxPrecision: opts.useMaxPrecision,
		GroupByPrefixes: opts.groupByPrefixes,
	})

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) && !isIndexNotReady(err) || isShutdownError(err) {
			return
		}

		if status.Code(err) == codes.NotFound {
			if absent {
				// Coverage: an aggregate over a ledger outside the fleet must
				// resolve NotFound rather than sum an empty ledger.
				assert.Reachable("singleton_driver_model: aggregate on an absent ledger returned NotFound", internal.Details{"ledger": ledger})

				return
			}

			c.validateLedgerNotFound(maxTicket, ledger, "AggregateVolumes")

			return
		}

		if len(needed) > 0 {
			c.validateAggregate(maxTicket, ledger, filter, opts, needed, nil, err)

			return
		}

		assert.Unreachable("singleton_driver_model: AggregateVolumes returned unexpected error", internal.Details{
			"ledger": ledger,
			"absent": absent,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	if absent {
		assert.Unreachable("singleton_driver_model: aggregate served a ledger outside the fleet", internal.Details{"ledger": ledger})

		return
	}

	c.validateAggregate(maxTicket, ledger, filter, opts, needed, res, nil)
}

// genGroupPrefixes rolls the account prefixes an aggregate groups by, one read
// in four: one to three drawn from the address pool, with a prefix no account
// carries mixed in so an empty group is served too.
func genGroupPrefixes() []string {
	if !oneIn(4) {
		return nil
	}

	n := int(random.RandomChoice([]uint8{1, 2, 3}))
	out := make([]string, 0, n)

	for range n {
		if oneIn(4) {
			out = append(out, "no-such-prefix:")

			continue
		}

		out = append(out, poolName()+":")
	}

	return out
}

// aggOptions are the result-stage options an aggregate request may set. A
// non-empty groupByPrefixes moves every total into per-prefix groups, in the
// order the request listed them.
type aggOptions struct {
	collapseColors  bool
	useMaxPrecision bool
	groupByPrefixes []string
}

// prefixOf assigns an account to the first prefix that matches it, mirroring
// groupedAggregator.matchPrefix. An account matching none is left out of the
// result entirely.
func (o aggOptions) prefixOf(addr string) (string, bool) {
	for _, prefix := range o.groupByPrefixes {
		if strings.HasPrefix(addr, prefix) {
			return prefix, true
		}
	}

	return "", false
}

// aggPair is one bucket's summed volumes.
type aggPair struct{ in, out uint256.Int }

func (p *aggPair) zero() bool { return p.in.IsZero() && p.out.IsZero() }

func addAgg(sums map[assetColor]*aggPair, key assetColor, in, out *uint256.Int) {
	p := sums[key]
	if p == nil {
		p = &aggPair{}
		sums[key] = p
	}

	p.in.Add(&p.in, in)
	p.out.Add(&p.out, out)
}

// modelAggregate folds the volume cells of every filter-matching account into
// per-(asset, color) sums the way the server's result stage does: precisions
// of one base merged under the highest seen with lower amounts rescaled, then
// colors summed into the uncolored bucket, each when asked. Fully-zero sums are
// dropped (a purge can zero a cell the server no longer reports).
func modelAggregate(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts aggOptions) map[assetColor]*aggPair {
	return aggregateFold(ls, filter, opts, nil)
}

// modelAggregateGroups is the per-prefix prediction: one entry per requested
// prefix, in request order, each folding the accounts that prefix claims. A
// prefix no account matches still gets its (empty) entry.
func modelAggregateGroups(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts aggOptions) []aggGroup {
	groups := make([]aggGroup, 0, len(opts.groupByPrefixes))

	for _, prefix := range opts.groupByPrefixes {
		sums := aggregateFold(ls, filter, opts, func(addr string) bool {
			claimed, ok := opts.prefixOf(addr)

			return ok && claimed == prefix
		})
		groups = append(groups, aggGroup{prefix: prefix, sums: sums})
	}

	return groups
}

// aggGroup is one prefix's totals.
type aggGroup struct {
	prefix string
	sums   map[assetColor]*aggPair
}

// aggregateFold sums the volume cells of the accounts filter selects and keep
// admits. A nil keep admits every matching account.
func aggregateFold(ls oracle.LedgerState, filter *commonpb.QueryFilter, opts aggOptions, keep func(addr string) bool) map[assetColor]*aggPair {
	sums := map[assetColor]*aggPair{}

	for k, vp := range ls.Volumes().All() {
		if filter != nil && !matchAccountFilter(ls, filter, k.Address) {
			continue
		}

		if keep != nil && !keep(k.Address) {
			continue
		}

		addAgg(sums, assetColor{Asset: k.Asset, Color: k.Color}, &vp.Input, &vp.Output)
	}

	if opts.useMaxPrecision {
		sums = mergePrecisions(sums)
	}

	if opts.collapseColors {
		collapsed := map[assetColor]*aggPair{}
		for key, p := range sums {
			addAgg(collapsed, assetColor{Asset: key.Asset}, &p.in, &p.out)
		}

		sums = collapsed
	}

	for key, p := range sums {
		if p.zero() {
			delete(sums, key)
		}
	}

	return sums
}

// mergePrecisions re-keys every bucket of a base under the base's highest
// precision, scaling lower-precision amounts by the power of ten between.
func mergePrecisions(sums map[assetColor]*aggPair) map[assetColor]*aggPair {
	maxPrecision := map[string]uint8{}
	for key := range sums {
		base, precision := splitAsset(key.Asset)
		maxPrecision[base] = max(maxPrecision[base], precision)
	}

	merged := map[assetColor]*aggPair{}
	for key, p := range sums {
		base, precision := splitAsset(key.Asset)
		target := maxPrecision[base]
		factor := uint256.NewInt(1)
		for range target - precision {
			factor.Mul(factor, uint256.NewInt(10))
		}

		var in, out uint256.Int
		in.Mul(&p.in, factor)
		out.Mul(&p.out, factor)
		addAgg(merged, assetColor{Asset: formatAsset(base, target), Color: key.Color}, &in, &out)
	}

	return merged
}

// splitAsset reads the server's asset convention: "BASE/N" carries precision
// N, a bare base has precision 0.
func splitAsset(asset string) (base string, precision uint8) {
	if i := strings.LastIndexByte(asset, '/'); i >= 0 {
		n, err := strconv.ParseUint(asset[i+1:], 10, 8)
		if err == nil {
			return asset[:i], uint8(n)
		}
	}

	return asset, 0
}

func formatAsset(base string, precision uint8) string {
	if precision == 0 {
		return base
	}

	return base + "/" + strconv.FormatUint(uint64(precision), 10)
}

// serverAggregate decodes an AggregateResult into per-(asset, color) sums. Every
// bucket the server sent is kept, zero totals included: a bucket it invented is
// a divergence the model side's drop would otherwise absorb. ok is false when a
// bucket repeats, which the result's one-entry-per-bucket contract forbids.
func serverAggregate(res *commonpb.AggregateResult) (out map[assetColor]*aggPair, ok bool) {
	out = map[assetColor]*aggPair{}

	for _, av := range res.GetVolumes() {
		key := assetColor{Asset: av.GetAsset(), Color: av.GetColor()}
		if _, dup := out[key]; dup {
			return nil, false
		}

		p := &aggPair{}
		av.GetInput().IntoUint256(&p.in)
		av.GetOutput().IntoUint256(&p.out)
		out[key] = p
	}

	return out, true
}

// serverAggregateGroups decodes the grouped arm of a result, preserving the
// order the server listed the prefixes in. ok is false when a group repeats a
// bucket.
func serverAggregateGroups(res *commonpb.AggregateResult) (out []aggGroup, ok bool) {
	for _, g := range res.GetGroups() {
		sums, decoded := serverAggregate(&commonpb.AggregateResult{Volumes: g.GetVolumes()})
		if !decoded {
			return nil, false
		}

		out = append(out, aggGroup{prefix: g.GetPrefix(), sums: sums})
	}

	return out, true
}

// aggGroupsEqual compares two grouped folds prefix for prefix, in order: the
// server emits one group per requested prefix, in request order.
func aggGroupsEqual(a, b []aggGroup) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i].prefix != b[i].prefix || !aggEqual(a[i].sums, b[i].sums) {
			return false
		}
	}

	return true
}

func renderAggGroups(groups []aggGroup) string {
	parts := make([]string, 0, len(groups))
	for _, g := range groups {
		parts = append(parts, g.prefix+"{"+renderAgg(g.sums)+"}")
	}

	return strings.Join(parts, " ")
}

func aggEqual(a, b map[assetColor]*aggPair) bool {
	if len(a) != len(b) {
		return false
	}

	for key, pa := range a {
		pb, ok := b[key]
		if !ok || pa.in.Cmp(&pb.in) != 0 || pa.out.Cmp(&pb.out) != 0 {
			return false
		}
	}

	return true
}

func renderAgg(m map[assetColor]*aggPair) string {
	keys := make([]assetColor, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=in:%s,out:%s", k.String(), m[k].in.Dec(), m[k].out.Dec()))
	}

	return strings.Join(parts, " ")
}

// validateAggregate checks the aggregate against the model: legal iff some
// candidate base holds every index the filter reads and its fold under the
// request's options equals the server's, bucket for bucket — or, when an index
// is not ready on that base, iff the server refused the read.
func (c *Checker) validateAggregate(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, opts aggOptions, needed map[string]struct{}, res *commonpb.AggregateResult, err error) {
	errKind, classified := classifyIndexedQueryError(err)
	if !classified {
		assert.Unreachable("singleton_driver_model: aggregate returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	grouped := len(opts.groupByPrefixes) > 0

	var (
		serverFlat   map[assetColor]*aggPair
		serverGroups []aggGroup
	)

	if errKind == indexedErrNone {
		// The two arms are exclusive: grouping moves every total into groups.
		if grouped && len(res.GetVolumes()) != 0 || !grouped && len(res.GetGroups()) != 0 {
			assert.Unreachable("singleton_driver_model: aggregate result mixes grouped and flat volumes", internal.Details{
				"ledger":   ledger,
				"prefixes": strings.Join(opts.groupByPrefixes, ","),
				"volumes":  len(res.GetVolumes()),
				"groups":   len(res.GetGroups()),
			})

			return
		}

		var decoded bool
		if grouped {
			serverGroups, decoded = serverAggregateGroups(res)
		} else {
			serverFlat, decoded = serverAggregate(res)
		}

		if !decoded {
			assert.Unreachable("singleton_driver_model: aggregate result repeats a bucket", internal.Details{
				"ledger": ledger,
				"filter": describeFilter(filter),
			})

			return
		}
	}

	rejectedIndex := rejectedIndexLabel(err)
	matched := c.matchesModel(maxTicket, "AGG", func(base oracle.GlobalState) bool {
		return indexedQueryOutcomeLegal(base.Ledger(ledger), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, filter, needed, errKind, rejectedIndex, func(ls oracle.LedgerState) bool {
			if grouped {
				return aggGroupsEqual(modelAggregateGroups(ls, filter, opts), serverGroups)
			}

			return aggEqual(modelAggregate(ls, filter, opts), serverFlat)
		})
	})

	if matched {
		switch {
		case errKind != indexedErrNone:
			// Coverage: an aggregate refused while an index it reads is not ready.
			assert.Reachable("singleton_driver_model: aggregate gated on a missing index", internal.Details{"ledger": ledger})
		case grouped:
			// Coverage: a prefix-grouped aggregate matched a candidate base.
			assert.Reachable("singleton_driver_model: grouped aggregate volumes validated", internal.Details{"ledger": ledger})
		default:
			// Coverage: an aggregate answered and matched a candidate base.
			assert.Reachable("singleton_driver_model: aggregate volumes validated", internal.Details{"ledger": ledger})
		}

		return
	}

	c.mu.Lock()
	committed := c.modelState.Ledger(ledger)
	c.mu.Unlock()

	details := internal.Details{
		"ledger":          ledger,
		"filter":          describeFilter(filter),
		"collapseColors":  opts.collapseColors,
		"useMaxPrecision": opts.useMaxPrecision,
		"prefixes":        strings.Join(opts.groupByPrefixes, ","),
	}
	if err != nil {
		details["error"] = err.Error()
	}

	if grouped {
		details["server"], details["model"] = renderAggGroups(serverGroups), renderAggGroups(modelAggregateGroups(committed, filter, opts))
	} else {
		details["server"], details["model"] = renderAgg(serverFlat), renderAgg(modelAggregate(committed, filter, opts))
	}

	assert.Unreachable("singleton_driver_model: aggregate volumes outside model", details)
}
