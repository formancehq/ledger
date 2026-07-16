package create

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil"
	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil/flagbind"
)

// NewCommand returns the "create" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var (
		dryRun    bool
		setValues []string
	)

	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a new Cluster deployment",
		Long:  "Create a new Cluster deployment.\nUse --set to configure spec fields (see 'explain' for available fields).\nExample: kubectl-ledger create my-ledger --set replicas=3 --set image.tag=v2.0",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, opts, setValues, dryRun, args)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print YAML without applying")
	flagbind.RegisterSetFlag(cmd, &setValues)

	return cmd
}

func runCreate(cmd *cobra.Command, opts *cmdutil.Options, setValues []string, dryRun bool, args []string) error {
	name, err := resolveName(args)
	if err != nil {
		return err
	}

	ns, err := resolveNamespace(cmd, opts)
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	overrides, err := flagbind.Collect(setValues)
	if err != nil {
		return err
	}

	// Apply create-specific defaults for fields not explicitly set.
	if _, ok := overrides["replicas"]; !ok {
		overrides["replicas"] = "1"
	}
	if _, ok := overrides["clusterID"]; !ok {
		overrides["clusterID"] = uuid.New().String()
	}
	if img, ok := overrides["image"].(map[string]any); !ok || img["repository"] == nil {
		flagbind.SetNested(overrides, []string{"image", "repository"}, "ghcr.io/formancehq/ledger")
	}
	if img, ok := overrides["image"].(map[string]any); !ok || img["tag"] == nil {
		flagbind.SetNested(overrides, []string{"image", "tag"}, "latest")
	}

	ledger := &ledgerv1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "Cluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
	}

	if err := flagbind.ApplyToStruct(&ledger.Spec, overrides); err != nil {
		return fmt.Errorf("applying spec: %w", err)
	}

	// Show preview.
	pterm.Println()
	pterm.DefaultSection.Println("Create Preview")
	previewRows := [][]string{
		{"Name", pterm.Cyan(name)},
		{"Namespace", ns},
	}
	previewRows = append(previewRows, flagbind.PreviewRows(overrides, "")...)
	cmdutil.RenderBoxedTable(previewRows)
	pterm.Println()

	if dryRun {
		b, err := yaml.Marshal(ledger)
		if err != nil {
			return fmt.Errorf("marshaling YAML: %w", err)
		}
		pterm.Info.Println("Dry run - YAML output:")
		pterm.Println()
		fmt.Print(string(b))

		return nil
	}

	confirm, err := cmdutil.PromptConfirm("Create this Cluster?", true)
	if err != nil {
		return err
	}
	if !confirm {
		pterm.Warning.Println("Aborted.")

		return nil
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Creating Cluster...")

	if err := crdClient.Create(cmd.Context(), ledger); err != nil {
		spinner.Fail("Failed to create Cluster")

		return fmt.Errorf("creating ledger %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("Cluster %s created in namespace %s", pterm.Cyan(name), pterm.Cyan(ns)))

	return nil
}

func resolveName(args []string) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}

	name, err := cmdutil.PromptText("Cluster name", "")
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", errors.New("ledger name is required")
	}

	return name, nil
}

func resolveNamespace(cmd *cobra.Command, opts *cmdutil.Options) (string, error) {
	if cmd.Flags().Changed("namespace") {
		return opts.ResolvedNamespace()
	}

	currentNS, err := opts.ResolvedNamespace()
	if err != nil {
		return "", err
	}

	namespaces, err := listNamespaces(cmd.Context(), opts)
	if err != nil {
		return cmdutil.PromptText("Namespace", currentNS)
	}

	selected, err := cmdutil.SelectPrompt("Namespace", namespaces)
	if err != nil {
		return "", fmt.Errorf("namespace selection failed: %w", err)
	}

	return selected, nil
}

func listNamespaces(ctx context.Context, opts *cmdutil.Options) ([]string, error) {
	clientset, err := opts.Clientset()
	if err != nil {
		return nil, err
	}

	nsList, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(nsList.Items))
	for i := range nsList.Items {
		names = append(names, nsList.Items[i].Name)
	}
	sort.Strings(names)

	return names, nil
}
