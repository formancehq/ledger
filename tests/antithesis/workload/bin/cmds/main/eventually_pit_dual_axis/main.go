package main

import (
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	ctx, cancel := internal.DriverContext()
	defer cancel()

	perNode, dialErr := internal.DialPerNode(ctx, true)
	if len(perNode) > 0 {
		defer perNode.Close()
	}
	resolved := internal.ResolvedPITScopeTargets(perNode)
	var (
		fixtureErr error
		oracle     *internal.PITDualAxisOracle
	)
	for oracle == nil && ctx.Err() == nil {
		for _, target := range resolved {
			oracle, fixtureErr = internal.LoadPITDualAxisOracle(ctx, target.Bucket)
			if oracle != nil {
				break
			}
			if fixtureErr != nil &&
				!internal.IsTransient(fixtureErr) &&
				!internal.IsCanceled(fixtureErr) &&
				!internal.IsPITDualAxisFixtureIncomplete(fixtureErr) {
				assert.Unreachable(
					"pit: dual-axis quiescent oracle could not authenticate its fixture",
					internal.Details{"node_address": target.Addr, "node_id": target.NodeID, "error": fixtureErr},
				)

				return
			}
		}
		if oracle == nil {
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	completed := make(map[string]struct{})
	expected := 0
	if oracle != nil {
		cases := internal.PITDualAxisCases(oracle)
		expected = len(resolved) * len(cases)
		for len(completed) < expected && ctx.Err() == nil {
			progressed := false
			for _, target := range resolved {
				for _, testCase := range cases {
					key := target.Addr + "\x00" + testCase.Name
					if _, ok := completed[key]; ok {
						continue
					}
					if internal.ComparePITDualAxisCase(
						internal.WithStaleConsistency(ctx),
						target.Bucket,
						target.Addr,
						target.NodeID,
						oracle,
						testCase,
						oracle.MaxLogSequence(),
					) {
						completed[key] = struct{}{}
						progressed = true
					}
				}
			}
			if !progressed {
				select {
				case <-ctx.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}

	assert.Sometimes(
		expected > 0 && len(completed) == expected,
		"pit: dual-axis oracle covered every boundary and scope on every replica",
		internal.Details{
			"completed":      len(completed),
			"expected":       expected,
			"resolved_nodes": len(resolved),
			"dial_error":     dialErr,
			"fixture_error":  fixtureErr,
		},
	)
}
