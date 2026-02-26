package defaults

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

func newListCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List LedgerDefaults resources",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd, opts)
		},
	}
}

func runList(cmd *cobra.Command, opts *cmdutil.Options) error {
	ctx := cmd.Context()

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching LedgerDefaults resources...")

	defaults, err := cmdutil.ListLedgerDefaults(ctx, crdClient)
	if err != nil {
		spinner.Fail("Failed to list LedgerDefaults resources")
		return fmt.Errorf("listing ledger defaults: %w", err)
	}

	_ = spinner.Stop()

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(defaults)
	case "yaml":
		return cmdutil.OutputYAML(defaults)
	default:
		return renderListTable(defaults)
	}
}

func renderListTable(defaults *ledgerv1alpha1.LedgerDefaultsList) error {
	header := []string{"NAME", "IMAGE", "RESOURCES", "MONITORING", "TLS", "AGE"}

	rows := make([][]string, 0, len(defaults.Items))
	for i := range defaults.Items {
		d := &defaults.Items[i]
		image := formatDefaultsImage(d.Spec.Image)
		resources := formatDefaultsResources(d.Spec.Resources)
		monitoring := formatBoolPresence(d.Spec.Config.Monitoring != nil)
		tls := formatBoolPresence(d.Spec.Config.TLS != nil && d.Spec.Config.TLS.Enabled)
		age := cmdutil.FormatAge(time.Since(d.CreationTimestamp.Time))

		rows = append(rows, []string{pterm.Cyan(d.Name), image, resources, monitoring, tls, age})
	}

	if len(rows) == 0 {
		pterm.Info.Println("No LedgerDefaults resources found.")
		pterm.Println(pterm.Gray("Create one with: kubectl ledger defaults create"))
		return nil
	}

	pterm.Println()
	cmdutil.RenderTable(header, rows)
	return nil
}

func formatDefaultsImage(img ledgerv1alpha1.ImageSpec) string {
	if img.Repository == "" && img.Tag == "" {
		return pterm.Gray("<not set>")
	}
	return cmdutil.FormatImage(img)
}

func formatDefaultsResources(r corev1.ResourceRequirements) string {
	if r.Requests == nil && r.Limits == nil {
		return pterm.Gray("<not set>")
	}
	return "configured"
}

func formatBoolPresence(v bool) string {
	if v {
		return pterm.Green("yes")
	}
	return pterm.Gray("no")
}
