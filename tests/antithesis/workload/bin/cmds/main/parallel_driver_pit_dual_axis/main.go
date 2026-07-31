package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	ctx, cancel := internal.DriverContext()
	defer cancel()
	details := internal.Details{}
	compared := false
	defer func() {
		assert.Sometimes(
			compared,
			"pit: dual-axis oracle compared a fault-time immutable view",
			details,
		)
	}()

	perNode, err := internal.DialPerNode(ctx, false)
	details["dial_error"] = err
	if err != nil || len(perNode) == 0 {
		return
	}
	defer perNode.Close()

	resolved := internal.ResolvedPITScopeTargets(perNode)
	if len(resolved) == 0 {
		return
	}
	target := resolved[internal.Rand().Uint64()%uint64(len(resolved))]
	details["node_address"] = target.Addr
	details["node_id"] = target.NodeID
	oracle, err := internal.LoadPITDualAxisOracle(ctx, target.Bucket)
	details["fixture_error"] = err
	if err != nil {
		if internal.IsTransient(err) || internal.IsCanceled(err) || internal.IsPITDualAxisFixtureIncomplete(err) {
			return
		}
		assert.Unreachable(
			"pit: dual-axis fault-time oracle could not authenticate its fixture",
			internal.Details{"node_address": target.Addr, "node_id": target.NodeID, "error": err},
		)

		return
	}
	cases := internal.PITDualAxisCases(oracle)
	testCase := cases[internal.Rand().Uint64()%uint64(len(cases))]
	details["case"] = testCase.Name
	compared = internal.ComparePITDualAxisCase(
		internal.WithStaleConsistency(ctx),
		target.Bucket,
		target.Addr,
		target.NodeID,
		oracle,
		testCase,
		0,
	)
}
