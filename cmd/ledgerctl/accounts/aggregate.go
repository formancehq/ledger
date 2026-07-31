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

// pointInTimeViewTrailerKey mirrors the gRPC wire contract without importing
// the server adapter package into the lightweight ledgerctl binary.
const pointInTimeViewTrailerKey = "x-point-in-time-view-bin"

type pointInTimeViewDisplay struct {
	RequestedAt          string `json:"requestedAt"`
	Axis                 string `json:"axis"`
	LedgerID             uint32 `json:"ledgerId"`
	AuditWatermark       uint64 `json:"auditWatermark"`
	LogWatermark         uint64 `json:"logWatermark"`
	ManifestVersion      uint64 `json:"manifestVersion"`
	HistoryAvailableFrom string `json:"historyAvailableFrom"`
	ViewToken            string `json:"viewToken"`
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
  ledgerctl accounts aggregate-volumes --ledger my-ledger --pit 2026-01-15T12:00:00Z
  ledgerctl accounts aggregate-volumes --ledger my-ledger --pit 2026-01-15T12:00:00Z --use-insertion-date
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
	cmd.Flags().String("pit", "", "Aggregate balances at this RFC3339 timestamp")
	cmd.Flags().Bool("use-insertion-date", false, "Interpret --pit on the insertion-date axis instead of the effective-date axis")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runAggregateVolumes(cmd *cobra.Command, _ []string) error {
	pointInTime, err := pointInTimeSelectorFromFlags(cmd)
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

	spinner, _ := pterm.DefaultSpinner.Start("Aggregating volumes...")

	var trailer metadata.MD

	result, err := client.AggregateVolumes(ctx, buildAggregateVolumesRequest(
		ledgerName,
		filter,
		minLogSeq,
		checkpointID,
		pointInTime,
	), grpc.Trailer(&trailer))
	_ = spinner.Stop()

	if err != nil {
		return cmdutil.FormatGRPCError("failed to aggregate volumes", err)
	}
	if pointInTime != nil {
		view, err := pointInTimeViewFromTrailer(trailer)
		if err != nil {
			return fmt.Errorf("invalid point-in-time response: %w", err)
		}
		if err := validatePointInTimeView(pointInTime, view); err != nil {
			return fmt.Errorf("invalid point-in-time response: %w", err)
		}
		if err := emitPointInTimeView(cmd, view); err != nil {
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

func pointInTimeSelectorFromFlags(cmd *cobra.Command) (*servicepb.PointInTimeSelector, error) {
	pit, err := cmd.Flags().GetString("pit")
	if err != nil {
		return nil, err
	}

	useInsertionDate, err := cmd.Flags().GetBool("use-insertion-date")
	if err != nil {
		return nil, err
	}

	checkpointID, err := cmd.Flags().GetUint64("checkpoint-id")
	if err != nil {
		return nil, err
	}

	if pit == "" {
		if useInsertionDate {
			return nil, errors.New("--use-insertion-date requires --pit")
		}

		return nil, nil
	}
	if checkpointID != 0 {
		return nil, errors.New("--pit and --checkpoint-id are mutually exclusive")
	}

	at, err := time.Parse(time.RFC3339Nano, pit)
	if err != nil {
		return nil, fmt.Errorf("invalid --pit value %q: expected RFC3339 timestamp: %w", pit, err)
	}
	if at.Before(time.Unix(0, 0)) {
		return nil, fmt.Errorf("invalid --pit value %q: timestamps before 1970-01-01T00:00:00Z are not supported", pit)
	}

	axis := servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE
	if useInsertionDate {
		axis = servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION
	}

	return &servicepb.PointInTimeSelector{
		At:   &commonpb.Timestamp{Data: uint64(at.UnixMicro())},
		Axis: axis,
	}, nil
}

func buildAggregateVolumesRequest(
	ledgerName string,
	filter *commonpb.QueryFilter,
	minLogSequence uint64,
	checkpointID uint64,
	pointInTime *servicepb.PointInTimeSelector,
) *servicepb.AggregateVolumesRequest {
	return &servicepb.AggregateVolumesRequest{
		Ledger:         ledgerName,
		Filter:         filter,
		MinLogSequence: minLogSequence,
		CheckpointId:   checkpointID,
		PointInTime:    pointInTime,
	}
}

func pointInTimeViewFromTrailer(trailer metadata.MD) (*servicepb.PointInTimeView, error) {
	values := trailer.Get(pointInTimeViewTrailerKey)
	if len(values) != 1 {
		return nil, fmt.Errorf("expected exactly one %s trailer value, got %d", pointInTimeViewTrailerKey, len(values))
	}

	view := &servicepb.PointInTimeView{}
	if err := view.UnmarshalVT([]byte(values[0])); err != nil {
		return nil, fmt.Errorf("decoding %s trailer: %w", pointInTimeViewTrailerKey, err)
	}
	if view.GetRequestedAt() == nil || view.GetHistoryAvailableFrom() == nil || view.GetViewToken() == "" {
		return nil, fmt.Errorf("%s trailer is incomplete", pointInTimeViewTrailerKey)
	}
	switch view.GetAxis() {
	case servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_EFFECTIVE,
		servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION:
	default:
		return nil, fmt.Errorf("%s trailer has unknown axis %d", pointInTimeViewTrailerKey, view.GetAxis())
	}

	return view, nil
}

func validatePointInTimeView(selector *servicepb.PointInTimeSelector, view *servicepb.PointInTimeView) error {
	if selector == nil || selector.GetAt() == nil {
		return errors.New("requested point-in-time selector is incomplete")
	}
	if view.GetRequestedAt().GetData() != selector.GetAt().GetData() {
		return fmt.Errorf(
			"response requestedAt %d does not match requested timestamp %d",
			view.GetRequestedAt().GetData(),
			selector.GetAt().GetData(),
		)
	}
	if view.GetAxis() != selector.GetAxis() {
		return fmt.Errorf(
			"response axis %s does not match requested axis %s",
			view.GetAxis(),
			selector.GetAxis(),
		)
	}

	return nil
}

func emitPointInTimeView(cmd *cobra.Command, view *servicepb.PointInTimeView) error {
	display := pointInTimeViewDisplay{
		RequestedAt:          view.GetRequestedAt().AsTime().UTC().Format(time.RFC3339Nano),
		Axis:                 pointInTimeAxisName(view.GetAxis()),
		LedgerID:             view.GetLedgerId(),
		AuditWatermark:       view.GetAuditWatermark(),
		LogWatermark:         view.GetLogWatermark(),
		ManifestVersion:      view.GetManifestVersion(),
		HistoryAvailableFrom: view.GetHistoryAvailableFrom().AsTime().UTC().Format(time.RFC3339Nano),
		ViewToken:            view.GetViewToken(),
	}

	encoded, err := json.Marshal(display)
	if err != nil {
		return fmt.Errorf("encoding point-in-time view: %w", err)
	}
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "point_in_time_view=%s\n", encoded); err != nil {
		return fmt.Errorf("displaying point-in-time view: %w", err)
	}

	return nil
}

func pointInTimeAxisName(axis servicepb.PointInTimeAxis) string {
	if axis == servicepb.PointInTimeAxis_POINT_IN_TIME_AXIS_INSERTION {
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
