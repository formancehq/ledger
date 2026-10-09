package accounts

import (
	"math/big"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewAggregateVolumesCommand creates the accounts aggregate-volumes command.
func NewAggregateVolumesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "aggregate-volumes",
		Aliases: []string{"agg"},
		Short:   "Aggregate volumes across accounts",
		Long: `Returns per-asset aggregated volumes (input, output, balance) for all accounts
matching the given filter. Same filter options as "accounts list".

Examples:
  ledgerctl accounts aggregate-volumes --ledger my-ledger
  ledgerctl accounts aggregate-volumes --ledger my-ledger --prefix users:
  ledgerctl accounts aggregate-volumes --ledger my-ledger --filter "metadata[type] == user"
  ledgerctl accounts agg --ledger my-ledger --json`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("prefix", "", "Filter accounts by address prefix (e.g. users:)")
	cmd.Flags().String("filter", "", `Filter expression (e.g. "metadata[category] == premium")`)
	cmdutil.AddOutputFlags(cmd)
	cmdutil.AddAnalyzeFlag(cmd)
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

// RenderAggregate writes aggregated volumes with decimal string amounts in
// structured output. Human output alone applies --rescale. The executor must
// request use_max_precision for that human path, as runAggregateVolumes does.
// The host owns --analyze profiling when calling this renderer directly.
func RenderAggregate(cmd *cobra.Command, result *commonpb.AggregateResult) error {
	return renderAggregate(cmd, result, nil, false)
}

func renderAggregate(cmd *cobra.Command, result *commonpb.AggregateResult, profile *servicepb.QueryProfile, showProfile bool) error {
	rescale := cmdutil.RescaleTarget(cmd)

	// Build (and, under --rescale, validate) the human-readable table before
	// printing anything, including the --analyze profile, so an invariant
	// failure aborts without partial output.
	var tableData pterm.TableData

	if !cmdutil.IsStructuredOutput(cmd) {
		var err error
		tableData, err = aggregateVolumesTable(result.GetVolumes(), rescale)
		if err != nil {
			return err
		}
	}

	if showProfile {
		output := cmd.OutOrStdout()
		if cmdutil.IsStructuredOutput(cmd) {
			output = cmd.ErrOrStderr()
		}
		cmdutil.RenderProfileTo(output, profile)
	}

	{
		type jsonVolume struct {
			Asset   string `json:"asset"`
			Color   string `json:"color"`
			Input   string `json:"input"`
			Output  string `json:"output"`
			Balance string `json:"balance"`
		}

		var volumes []jsonVolume

		for _, vol := range result.GetVolumes() {
			input := vol.GetInput().ToBigInt()
			output := vol.GetOutput().ToBigInt()
			balance := new(big.Int).Sub(input, output)
			volumes = append(volumes, jsonVolume{
				Asset:   vol.GetAsset(),
				Color:   vol.GetColor(),
				Input:   input.String(),
				Output:  output.String(),
				Balance: balance.String(),
			})
		}

		if handled, err := cmdutil.EncodeStructured(cmd, volumes); handled || err != nil {
			return err
		}
	}

	if len(result.GetVolumes()) == 0 {
		pterm.Info.WithWriter(cmd.OutOrStdout()).Println("No volumes found.")

		return nil
	}

	return pterm.DefaultTable.WithWriter(cmd.OutOrStdout()).WithHasHeader().WithData(tableData).Render()
}

// aggregateVolumesTable builds the ASSET/COLOR/INPUT/OUTPUT/BALANCE table for
// aggregate-volumes. It fails with an invariant error when --rescale meets an
// invalid asset rather than rendering it at the wrong precision.
func aggregateVolumesTable(vols []*commonpb.AggregatedVolume, rescale *uint8) (pterm.TableData, error) {
	tableData := pterm.TableData{
		{"ASSET", "COLOR", "INPUT", "OUTPUT", "BALANCE"},
	}

	// With --rescale, the server has already merged currencies that differ only
	// in precision into a single base-currency row expressed at the group's
	// highest precision (use_max_precision above). Colors stay segregated: they
	// are part of the server's merge key. The CLI only re-expresses each row at
	// the requested scale.
	if rescale != nil {
		for _, vol := range vols {
			base, precision, err := cmdutil.ParseAsset(vol.GetAsset())
			if err != nil {
				return nil, err
			}

			input := vol.GetInput().ToBigInt()
			output := vol.GetOutput().ToBigInt()
			balance := new(big.Int).Sub(input, output)

			tableData = append(tableData, []string{
				invariants.FormatAsset(base, *rescale),
				vol.GetColor(),
				cmdutil.RescaleAmount(input, precision, *rescale),
				cmdutil.RescaleAmount(output, precision, *rescale),
				withSign(cmdutil.RescaleAmount(balance, precision, *rescale), balance.Sign()),
			})
		}
	} else {
		for _, vol := range vols {
			input := vol.GetInput().ToBigInt()
			output := vol.GetOutput().ToBigInt()
			balance := new(big.Int).Sub(input, output)

			tableData = append(tableData, []string{
				vol.GetAsset(),
				vol.GetColor(),
				input.String(),
				output.String(),
				withSign(balance.String(), balance.Sign()),
			})
		}
	}

	return tableData, nil
}

// withSign prefixes a positive amount with '+' so credit/debit direction reads
// at a glance; zero and negative values are returned unchanged (negatives
// already carry '-').
func withSign(s string, sign int) string {
	if sign > 0 {
		return "+" + s
	}

	return s
}
