package accounts

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// historicalBalanceViewTrailerKey mirrors the gRPC wire contract without importing
// the server adapter package into the lightweight ledgerctl binary.
const historicalBalanceViewTrailerKey = "x-historical-balance-view-bin"

type historicalBalanceViewDisplay struct {
	RequestedAt     string `json:"requestedAt"`
	Temporality     string `json:"temporality"`
	Ledger          string `json:"ledger"`
	AuditWatermark  uint64 `json:"auditWatermark"`
	LogWatermark    uint64 `json:"logWatermark"`
	ManifestVersion uint64 `json:"manifestVersion"`
	ViewToken       string `json:"viewToken"`
}

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
  ledgerctl accounts aggregate-volumes --ledger my-ledger --at 2026-01-15T12:00:00Z
  ledgerctl accounts aggregate-volumes --ledger my-ledger --at 2026-01-15T12:00:00Z --temporality insertion
  ledgerctl accounts agg --ledger my-ledger --json`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runAggregateVolumes,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("prefix", "", "Filter accounts by address prefix (e.g. users:)")
	cmd.Flags().String("filter", "", `Filter expression (e.g. "metadata[category] == premium")`)
	cmdutil.AddOutputFlags(cmd)
	cmdutil.AddAnalyzeFlag(cmd)
	cmd.Flags().Uint64("min-log-sequence", 0, "Minimum log sequence the server must have applied before reading (0 = no constraint)")
	cmd.Flags().Uint64("checkpoint-id", 0, "Read from a query checkpoint instead of the live store")
	cmd.Flags().String("at", "", "Aggregate historical balances at this RFC3339 timestamp")
	cmd.Flags().String("temporality", "effective", "Timestamp temporality: effective or insertion")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAggregateVolumes(cmd *cobra.Command, _ []string) error {
	historicalBalance, err := historicalBalanceSelectorFromFlags(cmd)
	if err != nil {
		return err
	}

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

	filter, err := cmdutil.BuildQueryFilter(filterExpr, prefix, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if err != nil {
		return err
	}

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	if showProfile {
		ctx = cmdutil.ProfileContext(ctx)
	}

	spinner := cmdutil.StartSpinner("Aggregating volumes...")

	var trailer metadata.MD

	result, err := client.AggregateVolumes(ctx, buildAggregateVolumesRequest(
		ledgerName,
		filter,
		minLogSeq,
		checkpointID,
		historicalBalance,
	), grpc.Trailer(&trailer))
	_ = spinner.Stop()

	if err != nil {
		return cmdutil.FormatGRPCError("failed to aggregate volumes", err)
	}
	if historicalBalance != nil {
		view, err := historicalBalanceViewFromTrailer(trailer)
		if err != nil {
			return fmt.Errorf("invalid historical-balance response: %w", err)
		}
		if err := validateHistoricalBalanceView(historicalBalance, view); err != nil {
			return fmt.Errorf("invalid historical-balance response: %w", err)
		}
		if err := emitHistoricalBalanceView(cmd, view); err != nil {
			return err
		}
	}

	if showProfile {
		cmdutil.RenderProfile(cmdutil.ExtractProfile(trailer))
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
		pterm.Info.Println("No volumes found.")

		return nil
	}

	tableData := pterm.TableData{
		{"ASSET", "COLOR", "INPUT", "OUTPUT", "BALANCE"},
	}

	for _, vol := range result.GetVolumes() {
		input := vol.GetInput().ToBigInt()
		output := vol.GetOutput().ToBigInt()
		balance := new(big.Int).Sub(input, output)
		tableData = append(tableData, []string{
			vol.GetAsset(),
			vol.GetColor(),
			input.String(),
			output.String(),
			formatBalance(balance),
		})
	}

	_ = pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()

	return nil
}

func historicalBalanceSelectorFromFlags(cmd *cobra.Command) (*servicepb.HistoricalBalanceSelector, error) {
	atValue, err := cmd.Flags().GetString("at")
	if err != nil {
		return nil, err
	}

	temporalityValue, err := cmd.Flags().GetString("temporality")
	if err != nil {
		return nil, err
	}

	checkpointID, err := cmd.Flags().GetUint64("checkpoint-id")
	if err != nil {
		return nil, err
	}

	if atValue == "" {
		if cmd.Flags().Changed("temporality") {
			return nil, errors.New("--temporality requires --at")
		}

		return nil, nil
	}
	if checkpointID != 0 {
		return nil, errors.New("--at and --checkpoint-id are mutually exclusive")
	}

	at, err := time.Parse(time.RFC3339Nano, atValue)
	if err != nil {
		return nil, fmt.Errorf("invalid --at value %q: expected RFC3339 timestamp: %w", atValue, err)
	}
	if at.Before(time.Unix(0, 0)) {
		return nil, fmt.Errorf("invalid --at value %q: timestamps before 1970-01-01T00:00:00Z are not supported", atValue)
	}

	var temporality servicepb.HistoricalBalanceTemporality
	switch temporalityValue {
	case "effective":
		temporality = servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE
	case "insertion":
		temporality = servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION
	default:
		return nil, fmt.Errorf("invalid --temporality value %q: expected effective or insertion", temporalityValue)
	}

	return &servicepb.HistoricalBalanceSelector{
		At:          &commonpb.Timestamp{Data: uint64(at.UnixMicro())},
		Temporality: temporality,
	}, nil
}

func buildAggregateVolumesRequest(
	ledgerName string,
	filter *commonpb.QueryFilter,
	minLogSequence uint64,
	checkpointID uint64,
	historicalBalance *servicepb.HistoricalBalanceSelector,
) *servicepb.AggregateVolumesRequest {
	return &servicepb.AggregateVolumesRequest{
		Ledger:            ledgerName,
		Filter:            filter,
		MinLogSequence:    minLogSequence,
		CheckpointId:      checkpointID,
		HistoricalBalance: historicalBalance,
	}
}

func historicalBalanceViewFromTrailer(trailer metadata.MD) (*servicepb.HistoricalBalanceView, error) {
	values := trailer.Get(historicalBalanceViewTrailerKey)
	if len(values) != 1 {
		return nil, fmt.Errorf("expected exactly one %s trailer value, got %d", historicalBalanceViewTrailerKey, len(values))
	}

	view := &servicepb.HistoricalBalanceView{}
	if err := view.UnmarshalVT([]byte(values[0])); err != nil {
		return nil, fmt.Errorf("decoding %s trailer: %w", historicalBalanceViewTrailerKey, err)
	}
	if view.GetRequestedAt() == nil || view.GetViewToken() == "" {
		return nil, fmt.Errorf("%s trailer is incomplete", historicalBalanceViewTrailerKey)
	}
	switch view.GetTemporality() {
	case servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION:
	default:
		return nil, fmt.Errorf("%s trailer has unknown temporality %d", historicalBalanceViewTrailerKey, view.GetTemporality())
	}

	return view, nil
}

func validateHistoricalBalanceView(selector *servicepb.HistoricalBalanceSelector, view *servicepb.HistoricalBalanceView) error {
	if selector == nil || selector.GetAt() == nil {
		return errors.New("requested historical-balance selector is incomplete")
	}
	if view.GetRequestedAt().GetData() != selector.GetAt().GetData() {
		return fmt.Errorf(
			"response requestedAt %d does not match requested timestamp %d",
			view.GetRequestedAt().GetData(),
			selector.GetAt().GetData(),
		)
	}
	if view.GetTemporality() != selector.GetTemporality() {
		return fmt.Errorf(
			"response temporality %s does not match requested temporality %s",
			view.GetTemporality(),
			selector.GetTemporality(),
		)
	}

	return nil
}

func emitHistoricalBalanceView(cmd *cobra.Command, view *servicepb.HistoricalBalanceView) error {
	display := historicalBalanceViewDisplay{
		RequestedAt:     view.GetRequestedAt().AsTime().UTC().Format(time.RFC3339Nano),
		Temporality:     historicalBalanceTemporalityName(view.GetTemporality()),
		Ledger:          view.GetLedger(),
		AuditWatermark:  view.GetAuditWatermark(),
		LogWatermark:    view.GetLogWatermark(),
		ManifestVersion: view.GetManifestVersion(),
		ViewToken:       view.GetViewToken(),
	}

	encoded, err := json.Marshal(display)
	if err != nil {
		return fmt.Errorf("encoding historical-balance view: %w", err)
	}
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "historical_balance_view=%s\n", encoded); err != nil {
		return fmt.Errorf("displaying historical-balance view: %w", err)
	}

	return nil
}

func historicalBalanceTemporalityName(temporality servicepb.HistoricalBalanceTemporality) string {
	if temporality == servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION {
		return "insertion"
	}

	return "effective"
}

// formatBalance formats a balance with a sign indicator.
func formatBalance(b *big.Int) string {
	if b.Sign() > 0 {
		return "+" + b.String()
	}

	return b.String()
}
