package provision

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/scenario"
)

type namedScenario struct {
	name string
	fn   scenario.ScenarioFunc
}

func NewRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run [scenario-name]",
		Short: "Run a provisioning scenario against the cluster",
		Long: `Run a pre-built provisioning scenario that creates ledgers, account types,
numscripts, and sample transactions against the connected cluster.

Use 'provision list' to see available scenarios.

Examples:
  ledgerctl provision run gaming-wallet --server localhost:8888 --insecure
  ledgerctl provision run marketplace
  ledgerctl provision run marketplace --scale 2.0
  ledgerctl provision run --all --workers 4`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runProvision,
	}

	cmd.Flags().Duration("timeout", 120*time.Second, "Request timeout (default 120s for provisioning)")
	cmd.Flags().Float64("scale", 1.0, "Scale factor for iteration counts (e.g. 2.0 doubles, 0.5 halves)")
	cmd.Flags().Bool("all", false, "Run all available scenarios")
	cmd.Flags().Int("workers", 4, "Number of parallel workers when using --all")

	return cmd
}

func runProvision(cmd *cobra.Command, args []string) error {
	all, _ := cmd.Flags().GetBool("all")

	if !all && len(args) == 0 {
		return errors.New("specify a scenario name or use --all")
	}

	if all && len(args) > 0 {
		return errors.New("--all and a scenario name are mutually exclusive")
	}

	var scenarios []namedScenario

	if all {
		for _, name := range scenario.List() {
			fn, _ := scenario.Get(name)
			scenarios = append(scenarios, namedScenario{name: name, fn: fn})
		}
	} else {
		name := args[0]
		fn, ok := scenario.Get(name)
		if !ok {
			pterm.Error.Printfln("Unknown scenario %q. Use 'provision list' to see available scenarios.", name)

			return cmdutil.Displayed(fmt.Errorf("unknown scenario: %s", name))
		}
		scenarios = append(scenarios, namedScenario{name: name, fn: fn})
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	scale, _ := cmd.Flags().GetFloat64("scale")
	workers, _ := cmd.Flags().GetInt("workers")

	if len(scenarios) == 1 {
		return runSingle(ctx, client, scale, scenarios[0].name, scenarios[0].fn)
	}

	return runParallel(ctx, client, scale, workers, scenarios)
}

func runSingle(ctx context.Context, client servicepb.BucketServiceClient, scale float64, name string, fn scenario.ScenarioFunc) error {
	spinner, _ := pterm.DefaultSpinner.Start("Running scenario: " + name)

	runner := scenario.NewRunner(ctx, client).
		WithScale(scale).
		WithLogger(func(step string) {
			spinner.UpdateText(step)
		})

	if err := fn(runner); err != nil {
		spinner.Fail("Scenario failed: " + err.Error())

		return cmdutil.Displayed(fmt.Errorf("scenario %s failed: %w", name, err))
	}

	spinner.Success("Scenario " + name + " completed successfully")

	return nil
}

func runParallel(ctx context.Context, client servicepb.BucketServiceClient, scale float64, workers int, scenarios []namedScenario) error {
	pterm.Info.Printfln("Running %d scenarios with %d workers", len(scenarios), workers)

	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, s := range scenarios {
		g.Go(func() error {
			runner := scenario.NewRunner(ctx, client).
				WithScale(scale).
				WithLogger(func(step string) {
					mu.Lock()
					pterm.Info.Printfln("[%s] %s", s.name, step)
					mu.Unlock()
				})

			if err := s.fn(runner); err != nil {
				mu.Lock()
				pterm.Error.Printfln("[%s] failed: %s", s.name, err)
				mu.Unlock()

				return fmt.Errorf("scenario %s failed: %w", s.name, err)
			}

			mu.Lock()
			pterm.Success.Printfln("[%s] completed", s.name)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return cmdutil.Displayed(err)
	}

	pterm.Success.Println("All scenarios completed successfully")

	return nil
}
