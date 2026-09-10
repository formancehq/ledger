package querycheckpoint

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

func newCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "create",
		Short:             "Create a query checkpoint via Raft consensus",
		Long:              "Create a query checkpoint that captures a physical Pebble snapshot of the current state. The checkpoint is replicated to all nodes and enables point-in-time queries.",
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runCreate,
	}

	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runCreate(cmd *cobra.Command, _ []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	structuredOutput := cmdutil.IsStructuredOutput(cmd)

	var spinner *cmdutil.Spinner
	if !structuredOutput {
		spinner = cmdutil.StartSpinner("Creating query checkpoint...")
	}

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
			},
		},
	}

	applyReq, err := cmdutil.BuildApplyRequest(cmd, requests...)
	if err != nil {
		if spinner != nil {
			spinner.Fail("Failed to sign request")
		}

		return cmdutil.Displayed(err)
	}

	// Apply does not return until the read index checkpoint is materialized on
	// the serving node, so a read at the returned ID succeeds immediately there.
	resp, err := client.Apply(ctx, applyReq)
	if err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return cmdutil.FormatGRPCError("query checkpoint creation failed", err)
	}

	if err := cmdutil.VerifyResponseSignatures(cmd, resp.GetLogs()); err != nil {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return fmt.Errorf("response signature verification failed: %w", err)
	}

	checkpointID, maxSequence, ok := actions.GetCreatedQueryCheckpoint(resp)
	if !ok {
		if spinner != nil {
			_ = spinner.Stop()
		}

		return errors.New("checkpoint creation log not found in response")
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Query checkpoint created (id=%d)", checkpointID))
	}

	if handled, err := cmdutil.EncodeStructured(cmd, struct {
		CheckpointID uint64 `json:"checkpointId"`
		MaxSequence  uint64 `json:"maxSequence"`
	}{
		CheckpointID: checkpointID,
		MaxSequence:  maxSequence,
	}); handled || err != nil {
		return err
	}

	return nil
}
