package main

import (
	"context"
	"fmt"
	"sort"
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
// filter (or the whole ledger) and checks the per-asset sums against the
// model's candidate bases. Colored buckets are skipped on both sides,
// mirroring accountMatches; grouping and precision merging stay off, so the
// result is the plain per-asset fold of the matching accounts' volume cells.
func runAggregateQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)

	// Index-free filters only: every roll is servable, so an error is never
	// the gate's legal refusal and the sums are computable on any base.
	var filter *commonpb.QueryFilter
	if random.RandomChoice([]uint8{0, 1, 2}) != 0 {
		filter = genAccountFilterFree(0)
	}

	c.mu.Lock()
	readID := c.registerRead()
	minSeq := c.observedFrontier()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	res, err := client.AggregateVolumes(readCtx, &servicepb.AggregateVolumesRequest{
		Ledger:         ledger,
		Filter:         filter,
		MinLogSequence: minSeq,
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

	c.validateAggregate(maxTicket, ledger, filter, res)
}

// aggPair is one asset's summed volumes.
type aggPair struct{ in, out uint256.Int }

func (p *aggPair) zero() bool { return p.in.IsZero() && p.out.IsZero() }

// modelAggregate folds the uncolored volume cells of every filter-matching
// account into per-asset sums, dropping fully-zero sums (a purge can zero a
// cell the server no longer reports).
func modelAggregate(ls oracle.LedgerState, filter *commonpb.QueryFilter) map[string]*aggPair {
	sums := map[string]*aggPair{}

	for k, vp := range ls.Volumes().All() {
		if filter != nil && !matchAccountFilter(ls, filter, k.Address) {
			continue
		}

		p := sums[k.Asset]
		if p == nil {
			p = &aggPair{}
			sums[k.Asset] = p
		}

		p.in.Add(&p.in, &vp.Input)
		p.out.Add(&p.out, &vp.Output)
	}

	for asset, p := range sums {
		if p.zero() {
			delete(sums, asset)
		}
	}

	return sums
}

// serverAggregate decodes an AggregateResult into per-asset sums, skipping
// colored buckets and fully-zero entries.
func serverAggregate(res *commonpb.AggregateResult) map[string]*aggPair {
	out := map[string]*aggPair{}

	for _, av := range res.GetVolumes() {
		if av.GetColor() != "" {
			continue
		}

		p := &aggPair{}
		av.GetInput().IntoUint256(&p.in)
		av.GetOutput().IntoUint256(&p.out)

		if p.zero() {
			continue
		}

		out[av.GetAsset()] = p
	}

	return out
}

func aggEqual(a, b map[string]*aggPair) bool {
	if len(a) != len(b) {
		return false
	}

	for asset, pa := range a {
		pb, ok := b[asset]
		if !ok || pa.in.Cmp(&pb.in) != 0 || pa.out.Cmp(&pb.out) != 0 {
			return false
		}
	}

	return true
}

func renderAgg(m map[string]*aggPair) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=in:%s,out:%s", k, m[k].in.Dec(), m[k].out.Dec()))
	}

	return strings.Join(parts, " ")
}

// validateAggregate checks the aggregate against the model: legal iff some
// candidate base's per-asset fold equals the server's, sum for sum.
func (c *Checker) validateAggregate(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, res *commonpb.AggregateResult) {
	server := serverAggregate(res)

	if c.matchesModel(maxTicket, "AGG", func(base oracle.GlobalState) bool {
		return aggEqual(modelAggregate(base.Ledger(ledger), filter), server)
	}) {
		// Coverage: an aggregate answered and matched a candidate base.
		assert.Reachable("singleton_driver_model: aggregate volumes validated", internal.Details{"ledger": ledger})

		return
	}

	c.mu.Lock()
	committed := modelAggregate(c.modelState.Ledger(ledger), filter)
	c.mu.Unlock()

	assert.Unreachable("singleton_driver_model: aggregate volumes outside model", internal.Details{
		"ledger": ledger,
		"filter": describeFilter(filter),
		"server": renderAgg(server),
		"model":  renderAgg(committed),
	})
}
