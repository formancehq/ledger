package block

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/scenario"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
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

// RunLoop calls Setup once per group, then executes a bounded number of passes
// over every block. The command must return so Test Composer can schedule the
// rest of the singleton suite during a short platform run.
func RunLoop(ctx context.Context, client servicepb.BucketServiceClient, groups []*scenario.BlockGroup) {
	// Collect all blocks and run setups. Retry on Unavailable since the
	// cluster may not have elected a leader yet at startup.
	var allBlocks []*scenario.Block
	for _, g := range groups {
		if g.Setup != nil {
			actions := g.Setup()
			if len(actions) > 0 {
				var setupErr error
				for ctx.Err() == nil {
					_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions...))
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

	const passes = 4
	log.Printf("scenario_blocks: running %d passes over %d active blocks", passes, len(allBlocks))

	registerBlockProperties(allBlocks)

	randFn := scenario.RandFunc(func() uint64 { return internal.Rand().Uint64() })

	for attempt := 0; attempt < passes*len(allBlocks); attempt++ {
		if ctx.Err() != nil {
			return
		}

		b := allBlocks[attempt%len(allBlocks)]
		details := internal.Details{"block": b.Name}

		resp, err := b.Run(ctx, client, randFn)
		switch {
		case err == nil:
			emitBlockSucceeded(b.Name, wasHit, details)
			CheckPostCommitVolumes(resp, details)
		case errors.Is(err, scenario.ErrSkip):
		case internal.IsUnavailable(err):
			log.Printf("scenario_blocks: %s unavailable (transient): %v", b.Name, err)
		case isFailedPrecondition(err):
			// FailedPrecondition is expected for idempotent retries
			// (e.g. reverting an already-reverted transaction).
			log.Printf("scenario_blocks: %s precondition failed (expected): %v", b.Name, err)
		default:
			emitBlockFailed(b.Name, wasHit, details.With(internal.Details{"error": err}))
			log.Printf("scenario_blocks: %s failed: %v", b.Name, err)
		}
	}

	log.Println("scenario_blocks: bounded passes completed")
}

// The per-block properties are data-driven, so the antithesis-go-instrumentor
// cannot catalogue them (it only resolves literal message arguments). They are
// instead registered at runtime through assert.AssertRaw — the same not-hit
// emission the generated catalog performs for literal assertions — and the
// hit-time emissions reuse the identical message/ID so they land on the
// registered property. AssertRaw is invisible to the instrumentor's scanner,
// so these call sites produce no anonymous catalog entries.
const (
	blockClass = "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block"
	blockFile  = "tests/antithesis/workload/internal/block/block.go"

	wasHit = true
	notHit = false
)

// registerBlockProperties pre-registers both properties of every block about
// to enter the run loop: "block X succeeded" is a must-hit Reachable — a block
// that never succeeds during the run fails it — and "block X failed" is an
// Unreachable that stands passing until a failure fires it.
func registerBlockProperties(blocks []*scenario.Block) {
	for _, b := range blocks {
		emitBlockSucceeded(b.Name, notHit, nil)
		emitBlockFailed(b.Name, notHit, nil)
	}
}

func emitBlockSucceeded(name string, hit bool, details internal.Details) {
	msg := fmt.Sprintf("block %s succeeded", name)
	assert.AssertRaw(true, msg, details, blockClass, "RunLoop", blockFile, 0, hit, true, "reachability", "Reachable", msg)
}

func emitBlockFailed(name string, hit bool, details internal.Details) {
	msg := fmt.Sprintf("block %s failed", name)
	assert.AssertRaw(false, msg, details, blockClass, "RunLoop", blockFile, 0, hit, false, "reachability", "Unreachable", msg)
}

// isAlreadyExists checks if the gRPC error code is AlreadyExists.
func isAlreadyExists(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.AlreadyExists
}

// isFailedPrecondition checks if the gRPC error code is FailedPrecondition.
func isFailedPrecondition(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.FailedPrecondition
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
	internal.CheckPostCommitVolumes(ct.GetTransaction().GetPostCommitVolumes(), details)
}
