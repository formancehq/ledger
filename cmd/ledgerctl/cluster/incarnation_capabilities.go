package cluster

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewIncarnationCapabilitiesCommand reports committed activation state to the
// stopped-upgrade operator before it releases the drain.
func NewIncarnationCapabilitiesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "incarnation-capabilities",
		Short:  "Read committed account incarnation capabilities",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE:   runIncarnationCapabilities,
	}
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runIncarnationCapabilities(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	response, err := client.Discovery(ctx, &servicepb.DiscoveryRequest{})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to read account incarnation capabilities", err)
	}
	state := struct {
		WritesActive    bool `json:"writesActive"`
		HistoryComplete bool `json:"historyComplete"`
	}{
		WritesActive:    capabilities.IsActive(response.GetClusterPolicy(), capabilities.AccountIncarnationWritesV1),
		HistoryComplete: capabilities.IsActive(response.GetClusterPolicy(), capabilities.AccountIncarnationHistoryCompleteV1),
	}
	if handled, err := cmdutil.EncodeStructured(cmd, state); handled || err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Account incarnation writes active: %t; history complete: %t\n", state.WritesActive, state.HistoryComplete)

	return err
}
