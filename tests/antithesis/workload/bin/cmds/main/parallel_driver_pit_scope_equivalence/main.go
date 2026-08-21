package main

import (
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	ctx, cancel := internal.DriverContext()
	defer cancel()

	perNode, err := internal.DialPerNode(ctx, true)
	if err != nil || len(perNode) == 0 {
		return
	}
	defer perNode.Close()

	resolved := internal.ResolvedPITScopeTargets(perNode)
	if len(resolved) == 0 {
		return
	}
	target := resolved[internal.Rand().Uint64()%uint64(len(resolved))]
	testCases := internal.PITScopeCases()
	testCase := testCases[internal.Rand().Uint64()%uint64(len(testCases))]
	internal.ComparePITScopeCase(
		internal.WithStaleConsistency(ctx),
		target.Bucket,
		target.Addr,
		target.NodeID,
		testCase,
	)
}
