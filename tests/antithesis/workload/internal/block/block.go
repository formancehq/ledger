package block

import (
	"context"
	"errors"
	"fmt"
	"log"

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
	// Collect all blocks and run setups. Retry on Unavailable since the
	// cluster may not have elected a leader yet at startup.
	var allBlocks []*scenario.Block
	for _, g := range groups {
		if g.Setup != nil {
			actions := g.Setup()
			if len(actions) > 0 {
				var setupErr error
				for {
					if ctx.Err() != nil {
						return
					}

					_, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: actions})
					if err == nil || isAlreadyExists(err) {
						break
					}
					if internal.IsUnavailable(err) {
						log.Printf("scenario_blocks: setup unavailable, retrying: %v", err)
						continue
					}

					setupErr = err
					log.Printf("scenario_blocks: setup failed: %v", err)

					break
				}
				assert.Always(setupErr == nil, "should be able to setup scenario", internal.Details{"error": setupErr})
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
		case internal.IsUnavailable(err):
			log.Printf("scenario_blocks: %s unavailable (transient): %v", b.Name, err)
		default:
			assert.Unreachable(fmt.Sprintf("block %s failed", b.Name), details.With(internal.Details{"error": err}))
			log.Printf("scenario_blocks: %s failed: %v", b.Name, err)
		}
	}
}

// isAlreadyExists checks if the gRPC error code is AlreadyExists.
func isAlreadyExists(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.AlreadyExists
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
