package update

import (
	"encoding/json"
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil/flagbind"
)

// NewCommand returns the "update" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var setValues []string

	cmd := &cobra.Command{
		Use:   "update [name]",
		Short: "Update a Cluster configuration",
		Long:  "Update fields of an existing Cluster.\nUse --set for all fields (see 'explain' for available fields).\nExample: kubectl-ledger update my-ledger --set replicas=5 --set debug=true",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUpdate(cmd, opts, setValues, args)
		},
	}

	flagbind.RegisterSetFlag(cmd, &setValues)

	return cmd
}

func runUpdate(cmd *cobra.Command, opts *cmdutil.Options, setValues []string, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveClusterName(ctx, opts, args)
	if err != nil {
		return err
	}

	overrides, err := flagbind.Collect(setValues)
	if err != nil {
		return err
	}
	if len(overrides) == 0 {
		pterm.Info.Println("No --set values provided. Nothing to change.")

		return nil
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetCluster(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	// Compute diff for preview.
	origJSON, err := json.Marshal(ledger.Spec)
	if err != nil {
		return fmt.Errorf("marshaling spec: %w", err)
	}
	var original map[string]any
	if err := json.Unmarshal(origJSON, &original); err != nil {
		return fmt.Errorf("unmarshaling spec: %w", err)
	}

	patch := client.MergeFrom(ledger.DeepCopy())

	if err := flagbind.ApplyToStruct(&ledger.Spec, overrides); err != nil {
		return fmt.Errorf("applying overrides: %w", err)
	}

	modJSON, err := json.Marshal(ledger.Spec)
	if err != nil {
		return fmt.Errorf("marshaling modified spec: %w", err)
	}
	var modified map[string]any
	if err := json.Unmarshal(modJSON, &modified); err != nil {
		return fmt.Errorf("unmarshaling modified spec: %w", err)
	}

	changes := cmdutil.ComputeDiff(original, modified, "spec")
	if len(changes) == 0 {
		pterm.Info.Println("No effective changes.")

		return nil
	}

	pterm.Println()
	pterm.DefaultSection.Println("Pending changes")
	rows := make([][]string, 0, len(changes))
	for _, c := range changes {
		rows = append(rows, []string{
			pterm.Cyan(c.Path),
			pterm.Gray(cmdutil.FormatChangeValue(c.Old)) + "  " + pterm.Yellow("→") + "  " + pterm.Green(cmdutil.FormatChangeValue(c.New)),
		})
	}
	cmdutil.RenderTable([]string{"FIELD", "CHANGE"}, rows)

	ok, err := cmdutil.PromptConfirm(fmt.Sprintf("Apply changes to Cluster %s?", name), true)
	if err != nil {
		return err
	}
	if !ok {
		pterm.Info.Println("Aborted.")

		return nil
	}

	if err := crdClient.Patch(ctx, ledger, patch); err != nil {
		return fmt.Errorf("patching ledger %q: %w", name, err)
	}

	pterm.Success.Printfln("Cluster %s updated", name)

	return nil
}
