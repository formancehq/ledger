package block

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Registry holds all registered block groups.
var registry []*scenario.BlockGroup

// Register adds a block group to the global registry.
func Register(g *scenario.BlockGroup) {
	registry = append(registry, g)
}

// All returns all registered blocks (flattened across groups).
func All() []*scenario.BlockGroup {
	return registry
}

// ForScenario returns groups whose blocks match the given scenario prefix.
// If name is empty, all groups are returned.
func ForScenario(name string) []*scenario.BlockGroup {
	if name == "" {
		return registry
	}
	var filtered []*scenario.BlockGroup
	for _, g := range registry {
		var matching []*scenario.Block
		for _, b := range g.Blocks {
			if scenario.BlockScenario(b.Name) == name {
				matching = append(matching, b)
			}
		}
		if len(matching) > 0 {
			filtered = append(filtered, &scenario.BlockGroup{
				Setup:  g.Setup,
				Blocks: matching,
			})
		}
	}
	return filtered
}

// Scenarios returns the list of distinct scenario names.
func Scenarios() []string {
	seen := make(map[string]bool)
	var names []string
	for _, g := range registry {
		for _, b := range g.Blocks {
			s := scenario.BlockScenario(b.Name)
			if !seen[s] {
				seen[s] = true
				names = append(names, s)
			}
		}
	}
	return names
}

// RunLoop picks random blocks and runs them in a loop.
// It calls Setup once per group, then loops forever picking random blocks.
func RunLoop(ctx context.Context, client servicepb.BucketServiceClient, groups []*scenario.BlockGroup) {
	// Collect all blocks and run setups.
	var allBlocks []*scenario.Block
	for _, g := range groups {
		if g.Setup != nil {
			actions := g.Setup()
			if len(actions) > 0 {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: actions})
				details := internal.Details{"error": err}
				assert.Sometimes(err == nil, "should be able to setup scenario", details)
				if err != nil && !isAlreadyExists(err) {
					log.Printf("scenario_blocks: setup failed: %v", err)
				}
			}
		}
		allBlocks = append(allBlocks, g.Blocks...)
	}

	if len(allBlocks) == 0 {
		log.Println("scenario_blocks: no blocks to run")
		return
	}

	log.Printf("scenario_blocks: %d blocks active, entering loop", len(allBlocks))

	randFn := scenario.RandFunc(random.GetRandom)

	for {
		if ctx.Err() != nil {
			return
		}

		b := allBlocks[random.GetRandom()%uint64(len(allBlocks))]
		details := internal.Details{"block": b.Name}

		resp, err := b.Run(ctx, client, randFn)
		switch {
		case err == nil:
			assert.Reachable(fmt.Sprintf("block %s succeeded", b.Name), details)
			CheckPostCommitVolumes(resp, details)
		case errors.Is(err, scenario.ErrSkip):
			assert.Sometimes(true, fmt.Sprintf("block %s precondition not met (skip)", b.Name), details)
		default:
			assert.Sometimes(false, fmt.Sprintf("block %s failed", b.Name), details.With(internal.Details{"error": err}))
			log.Printf("scenario_blocks: %s failed: %v", b.Name, err)
		}
	}
}

// isAlreadyExists checks if the gRPC error code is AlreadyExists.
func isAlreadyExists(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.AlreadyExists
}

// CheckDoubleEntry verifies the double-entry invariant (sum of all balances = 0 per asset)
// for a ledger and emits an Antithesis Always assertion.
func CheckDoubleEntry(ctx context.Context, client servicepb.BucketServiceClient, ledger string, details internal.Details) {
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger})
	if err != nil {
		return
	}

	sums := make(map[string]*big.Int)
	for {
		account, err := stream.Recv()
		if err != nil {
			break
		}
		for asset, vol := range account.Volumes {
			balance, _ := new(big.Int).SetString(vol.GetBalance(), 10)
			if balance == nil {
				balance = big.NewInt(0)
			}
			if sums[asset] == nil {
				sums[asset] = big.NewInt(0)
			}
			sums[asset].Add(sums[asset], balance)
		}
	}

	for asset, total := range sums {
		assert.Always(
			total.Cmp(big.NewInt(0)) == 0,
			"double-entry: sum of balances should be 0",
			details.With(internal.Details{"asset": asset, "sum": total.String()}),
		)
	}
}

// CheckPostCommitVolumes verifies volume consistency on a transaction response.
func CheckPostCommitVolumes(resp *servicepb.ApplyResponse, details internal.Details) {
	if resp == nil || len(resp.Logs) == 0 {
		return
	}
	applyLog := resp.Logs[0].Payload.GetApply()
	if applyLog == nil {
		return
	}
	ct := applyLog.Log.Data.GetCreatedTransaction()
	if ct == nil {
		return
	}
	internal.CheckPostCommitVolumes(ct.PostCommitVolumes, details)
}
