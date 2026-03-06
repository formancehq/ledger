package accounts

import (
	"encoding/json"
	"math/big"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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
		RunE: runAggregateVolumes,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("prefix", "", "Filter accounts by address prefix (e.g. users:)")
	cmd.Flags().String("filter", "", `Filter expression (e.g. "metadata[category] == premium")`)
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence the server must have applied before reading (0 = no constraint)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAggregateVolumes(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ledgerFlag, _ := cmd.Flags().GetString("ledger")

	ledgerName, err := cmdutil.SelectLedger(cmd, client, ledgerFlag)
	if err != nil {
		return err
	}

	prefix, _ := cmd.Flags().GetString("prefix")
	filterExpr, _ := cmd.Flags().GetString("filter")
	jsonOutput, _ := cmd.Flags().GetBool("json")
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")

	filter, err := buildAccountFilter(filterExpr, prefix)
	if err != nil {
		return err
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start("Aggregating volumes...")

	result, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
		Ledger:         ledgerName,
		Filter:         filter,
		MinLogSequence: minLogSeq,
	})
	_ = spinner.Stop()

	if err != nil {
		return cmdutil.FormatGRPCError("failed to aggregate volumes", err)
	}

	if jsonOutput {
		type jsonVolume struct {
			Asset   string `json:"asset"`
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
				Input:   input.String(),
				Output:  output.String(),
				Balance: balance.String(),
			})
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")

		return encoder.Encode(volumes)
	}

	if len(result.GetVolumes()) == 0 {
		pterm.Info.Println("No volumes found.")

		return nil
	}

	tableData := pterm.TableData{
		{"ASSET", "INPUT", "OUTPUT", "BALANCE"},
	}

	for _, vol := range result.GetVolumes() {
		input := vol.GetInput().ToBigInt()
		output := vol.GetOutput().ToBigInt()
		balance := new(big.Int).Sub(input, output)
		tableData = append(tableData, []string{
			vol.GetAsset(),
			input.String(),
			output.String(),
			formatBalance(balance),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	return nil
}

// formatBalance formats a balance with a sign indicator.
func formatBalance(b *big.Int) string {
	if b.Sign() > 0 {
		return "+" + b.String()
	}

	return b.String()
}
