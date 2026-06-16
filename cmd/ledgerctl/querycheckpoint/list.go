package querycheckpoint

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func newListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: cmdutil.ListAliases,
		Short:   "List all query checkpoints",
		Long: `List all query checkpoints registered on the cluster.

Query checkpoints are stored in replicated state and naturally bounded in size;
this endpoint is intentionally not paginated.`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runList,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runList(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	resp, err := client.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("listing query checkpoints failed", err)
	}

	if handled, err := cmdutil.EncodeStructured(cmd, resp.GetCheckpoints()); handled || err != nil {
		return err
	}

	if len(resp.GetCheckpoints()) == 0 {
		pterm.Info.Println("No query checkpoints found.")

		return nil
	}

	tableData := pterm.TableData{
		{"ID", "CREATED", "MAX SEQUENCE"},
	}

	for _, cp := range resp.GetCheckpoints() {
		created := cp.GetCreatedAt().AsTime().Format(time.RFC3339)

		tableData = append(tableData, []string{
			strconv.FormatUint(cp.GetCheckpointId(), 10),
			created,
			strconv.FormatUint(cp.GetMaxSequence(), 10),
		})
	}

	if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
		return fmt.Errorf("rendering table: %w", err)
	}

	return nil
}
