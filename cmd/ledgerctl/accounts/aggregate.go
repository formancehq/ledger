package accounts

import (
	"math/big"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
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
		RunE: runAggregateVolumes,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("prefix", "", "Filter accounts by address prefix (e.g. users:)")
	cmd.Flags().String("filter", "", `Filter expression (e.g. "metadata[category] == premium")`)
	cmdutil.AddOutputFlags(cmd)
	cmdutil.AddAnalyzeFlag(cmd)
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence the server must have applied before reading (0 = no constraint)")
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
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
	showProfile, _ := cmd.Flags().GetBool("analyze")
	minLogSeq, _ := cmd.Flags().GetUint64("min-log-sequence")
	checkpointID, _ := cmd.Flags().GetUint64("checkpoint-id")

	filter, err := buildAccountFilter(filterExpr, prefix)
	if err != nil {
		return err
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Aggregating volumes...")

	var trailer metadata.MD

	result, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
		Ledger:         ledgerName,
		Filter:         filter,
		MinLogSequence: minLogSeq,
		CheckpointId:   checkpointID,
	}, grpc.Trailer(&trailer))
	_ = spinner.Stop()

	if err != nil {
		return cmdutil.FormatGRPCError("failed to aggregate volumes", err)
	}

	if showProfile {
		cmdutil.RenderProfile(cmdutil.ExtractProfile(trailer))
	}

	{
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

		if handled, err := cmdutil.EncodeStructured(cmd, volumes); handled || err != nil {
			return err
		}
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
