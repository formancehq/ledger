package scale

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "scale" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var replicas int32

	cmd := &cobra.Command{
		Use:   "scale [name]",
		Short: "Scale a Cluster deployment",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScale(cmd, opts, &replicas, args)
		},
	}

	cmd.Flags().Int32Var(&replicas, "replicas", 0, "Number of replicas (must be odd)")

	return cmd
}

func runScale(cmd *cobra.Command, opts *cmdutil.Options, replicas *int32, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveClusterName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetCluster(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	currentReplicas := int32(3)
	if ledger.Spec.Replicas != nil {
		currentReplicas = *ledger.Spec.Replicas
	}

	pterm.Info.Printfln("Image: %s", cmdutil.FormatImage(ledger.Spec.Image))

	// Prompt for replicas if not explicitly set
	newReplicas := *replicas
	if !cmd.Flags().Changed("replicas") {
		pterm.Info.Printfln("Current replicas: %s", pterm.Cyan(strconv.Itoa(int(currentReplicas))))
		newReplicas, err = cmdutil.PromptReplicas(currentReplicas)
		if err != nil {
			return err
		}
	}

	if err := cmdutil.ValidateReplicas(newReplicas); err != nil {
		return err
	}

	if newReplicas == currentReplicas {
		pterm.Info.Printfln("Cluster %s is already at %d replicas", pterm.Cyan(name), currentReplicas)

		return nil
	}

	if newReplicas < currentReplicas {
		pterm.Warning.Printfln("Scaling down from %d to %d replicas", currentReplicas, newReplicas)
	}

	patch := client.MergeFrom(ledger.DeepCopy())
	ledger.Spec.Replicas = &newReplicas

	spinner, _ := pterm.DefaultSpinner.Start("Scaling Cluster...")

	if err := crdClient.Patch(ctx, ledger, patch); err != nil {
		spinner.Fail("Failed to scale Cluster")

		return fmt.Errorf("scaling ledger %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("Cluster %s scaled to %d replicas", pterm.Cyan(name), newReplicas))

	return nil
}
