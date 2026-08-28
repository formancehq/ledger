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
	"google.golang.org/grpc/metadata"

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
	ledger := random.RandomChoice(c.ledgerNames)

	// Index-free filters only: every roll is servable, so an error is never
	// the gate's legal refusal and the sums are computable on any base.
	var filter *commonpb.QueryFilter
	if random.RandomChoice([]uint8{0, 1, 2}) != 0 {
		filter = genAccountFilterFree(0)
	}

	opts := aggOptions{collapseColors: oneIn(3), useMaxPrecision: oneIn(4)}

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
	})

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		assert.Unreachable("singleton_driver_model: AggregateVolumes returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	c.validateAggregate(maxTicket, ledger, filter, opts, res)
}

// aggOptions are the result-stage options an aggregate request may set.
type aggOptions struct {
	collapseColors  bool
	useMaxPrecision bool
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
	sums := map[assetColor]*aggPair{}

	for k, vp := range ls.Volumes().All() {
		if filter != nil && !matchAccountFilter(ls, filter, k.Address) {
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

// serverAggregate decodes an AggregateResult into per-(asset, color) sums,
// dropping fully-zero entries. ok is false when a bucket repeats, which the
// result's one-entry-per-bucket contract forbids.
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

	for key, p := range out {
		if p.zero() {
			delete(out, key)
		}
	}

	return out, true
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
// candidate base's fold under the request's options equals the server's,
// bucket for bucket.
func (c *Checker) validateAggregate(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, opts aggOptions, res *commonpb.AggregateResult) {
	server, ok := serverAggregate(res)
	if !ok {
		assert.Unreachable("singleton_driver_model: aggregate result repeats a bucket", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
		})

		return
	}

	if c.matchesModel(maxTicket, "AGG", func(base oracle.GlobalState) bool {
		return aggEqual(modelAggregate(base.Ledger(ledger), filter, opts), server)
	}) {
		// Coverage: an aggregate answered and matched a candidate base.
		assert.Reachable("singleton_driver_model: aggregate volumes validated", internal.Details{"ledger": ledger})

		return
	}

	c.mu.Lock()
	committed := modelAggregate(c.modelState.Ledger(ledger), filter, opts)
	c.mu.Unlock()

	assert.Unreachable("singleton_driver_model: aggregate volumes outside model", internal.Details{
		"ledger":          ledger,
		"filter":          describeFilter(filter),
		"collapseColors":  opts.collapseColors,
		"useMaxPrecision": opts.useMaxPrecision,
		"server":          renderAgg(server),
		"model":           renderAgg(committed),
	})
}
