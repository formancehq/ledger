package credentials

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil/flagbind"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/explain"
)

func newCreateCommand(opts *cmdutil.Options) *cobra.Command {
	var (
		dryRun    bool
		setValues []string
	)

	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a new Credentials resource",
		Long:  "Create a new Credentials.\nExample: kubectl-ledger credentials create my-creds --set scopes=read,write --set selector.matchLabels.env=prod",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, opts, setValues, dryRun, args)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print YAML without applying")
	flagbind.RegisterSetFlag(cmd, &setValues, explain.CredentialsSpecFields)

	return cmd
}

func runCreate(cmd *cobra.Command, opts *cmdutil.Options, setValues []string, dryRun bool, args []string) error {
	name, err := resolveCredentialsName(args)
	if err != nil {
		return err
	}

	overrides, err := flagbind.Collect(setValues)
	if err != nil {
		return err
	}

	credentials := &ledgerv1alpha1.Credentials{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "Credentials",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	if err := flagbind.ApplyToStruct(&credentials.Spec, overrides); err != nil {
		return fmt.Errorf("applying spec: %w", err)
	}

	// Show preview.
	pterm.Println()
	pterm.DefaultSection.Println("Create Credentials Preview")
	previewRows := [][]string{
		{"Name", pterm.Cyan(name)},
		{"Scope", "Cluster"},
	}
	if len(credentials.Spec.Scopes) > 0 {
		previewRows = append(previewRows, []string{"Scopes", strings.Join(credentials.Spec.Scopes, ", ")})
	}
	if credentials.Spec.God {
		previewRows = append(previewRows, []string{"God Mode", pterm.Yellow("enabled")})
	}
	previewRows = append(previewRows, flagbind.PreviewRows(overrides, "", "scopes", "god")...)
	cmdutil.RenderBoxedTable(previewRows)
	pterm.Println()

	if dryRun {
		b, err := yaml.Marshal(credentials)
		if err != nil {
			return fmt.Errorf("marshaling YAML: %w", err)
		}
		pterm.Info.Println("Dry run - YAML output:")
		pterm.Println()
		fmt.Print(string(b))

		return nil
	}

	confirm, err := cmdutil.PromptConfirm("Create this Credentials?", true)
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

	spinner, _ := pterm.DefaultSpinner.Start("Creating Credentials...")

	if err := crdClient.Create(cmd.Context(), credentials); err != nil {
		spinner.Fail("Failed to create Credentials")

		return fmt.Errorf("creating credentials %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("Credentials %s created", pterm.Cyan(name)))

	return nil
}

func resolveCredentialsName(args []string) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}

	name, err := cmdutil.PromptText("Credentials name", "")
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", errors.New("name is required")
	}

	return name, nil
}
