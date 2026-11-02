package block_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/scenario"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/gaming"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/lending"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/marketplace"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/multicurrency"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/payroll"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/subscription"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func TestRegisteredBlocks(t *testing.T) {
	t.Parallel()
	var names []string
	for _, g := range block.All() {
		require.NotNil(t, g.Setup)
		for _, b := range g.Blocks {
			names = append(names, b.Name)
		}
	}
	require.ElementsMatch(t, []string{
		"gaming/topup", "gaming/buy_item", "gaming/p2p_trade", "gaming/promotion", "gaming/clawback",
		"lending/fund_pool", "lending/disburse", "lending/repay", "lending/provision", "lending/write_off",
		"marketplace/deposit", "marketplace/purchase", "marketplace/revert", "marketplace/payout", "marketplace/metadata",
		"multicurrency/fund", "multicurrency/fx", "multicurrency/vendor_pay",
		"payroll/fund_clearing", "payroll/distribute", "payroll/pay_salary", "payroll/cost_alloc",
		"subscription/fund", "subscription/charge", "subscription/recognize", "subscription/credit",
	}, names)
}

func TestRunLoopUsesCallbackSource(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := 0
	groups := []*scenario.BlockGroup{{Blocks: []*scenario.Block{{Name: "test/source", Run: func(_ context.Context, _ servicepb.BucketServiceClient, r scenario.Random) (*servicepb.ApplyResponse, error) {
		called++
		// Use the source only here, during the callback that owns it.
		_, ok := r.(*generation.Source)
		if !ok {
			panic("block did not receive the Hegel callback source")
		}
		if r.Intn(1) != 0 || r.Int63n(1) != 0 {
			panic("invalid bounded draw")
		}
		cancel()

		return nil, scenario.ErrSkip
	}}}}}
	block.RunLoop(ctx, nil, groups)
	require.Equal(t, 1, called)
}

func TestRunLoopReportsGenerationFailure(t *testing.T) {
	// The standard logger is process-global, so this test does not run in parallel.
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	called := 0
	groups := []*scenario.BlockGroup{{Blocks: []*scenario.Block{{Name: "test/failure", Run: func(context.Context, servicepb.BucketServiceClient, scenario.Random) (*servicepb.ApplyResponse, error) {
		called++
		panic("draw adapter failure")
	}}}}}
	block.RunLoop(context.Background(), nil, groups)
	require.Equal(t, 1, called, "the native failure replay must not repeat a block")
	require.Contains(t, output.String(), "scenario_blocks: generation failed:")
	require.Contains(t, output.String(), "draw adapter failure")
	require.NotContains(t, output.String(), "test/failure failed:", "generation failure must stay distinct from ordinary block errors")
}

func TestRunLoopKeepsBlockErrorsDistinct(t *testing.T) {
	// Capture the process-global logger only in a non-parallel test.
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"skip", scenario.ErrSkip, ""},
		{"unavailable", status.Error(codes.Unavailable, "temporarily unavailable"), "unavailable (transient)"},
		{"precondition", status.Error(codes.FailedPrecondition, "already reverted"), "precondition failed (expected)"},
		{"failure", errors.New("ordinary block failure"), "test/block failed: ordinary block failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(previous)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			groups := []*scenario.BlockGroup{{Blocks: []*scenario.Block{{Name: "test/block", Run: func(context.Context, servicepb.BucketServiceClient, scenario.Random) (*servicepb.ApplyResponse, error) {
				calls++
				cancel()

				return nil, tc.err
			}}}}}
			block.RunLoop(ctx, nil, groups)
			require.Equal(t, 1, calls)
			require.NotContains(t, output.String(), "generation failed")
			if tc.want != "" {
				require.Contains(t, output.String(), tc.want)
			}
		})
	}
}
