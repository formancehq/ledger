package main

import (
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	ctx, cancel := internal.DriverContext()
	defer cancel()

	perNode, err := internal.DialPerNode(ctx, true)
	testCases := internal.PITScopeCases()
	paired := make(map[string]struct{}, len(testCases))
	var nodeAddress string
	if err == nil && len(perNode) > 0 {
		defer perNode.Close()

		resolved := internal.ResolvedPITScopeTargets(perNode)
		if len(resolved) > 0 {
			target := resolved[internal.Rand().Uint64()%uint64(len(resolved))]
			nodeAddress = target.Addr
			readCtx := internal.WithStaleConsistency(ctx)
			for len(paired) < len(testCases) && ctx.Err() == nil {
				for _, testCase := range testCases {
					if _, exists := paired[testCase.Name]; exists {
						continue
					}
					if internal.ComparePITScopeCase(
						readCtx,
						target.Bucket,
						target.Addr,
						target.NodeID,
						testCase,
					) {
						paired[testCase.Name] = struct{}{}
					}
				}
				if len(paired) == len(testCases) {
					break
				}
				select {
				case <-ctx.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}

	assert.Sometimes(
		len(paired) == len(testCases),
		"pit: redundant aggregate scopes were compared in every transform mode",
		internal.Details{
			"ledger":       internal.PITScopeLedgerName(),
			"node_address": nodeAddress,
			"paired_cases": len(paired),
			"expected":     len(testCases),
			"dial_error":   err,
		},
	)
}
