package agents

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type createFlags struct {
	scopes   []string
	selector string
	dryRun   bool
}

func newCreateCommand(opts *cmdutil.Options) *cobra.Command {
	var f createFlags

	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a new LedgerClusterAgent resource",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, opts, &f, args)
		},
	}

	cmd.Flags().StringSliceVar(&f.scopes, "scopes", nil, "Comma-separated list of scopes (e.g. write,read)")
	cmd.Flags().StringVar(&f.selector, "selector", "", "Label selector in key=value,key2=value2 format")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "Print YAML without applying")

	return cmd
}

func runCreate(cmd *cobra.Command, opts *cmdutil.Options, f *createFlags, args []string) error {
	name, err := resolveAgentName(args)
	if err != nil {
		return err
	}

	// Interactive prompts for fields not set via flags.
	if err := runCreateWizard(cmd, f); err != nil {
		return err
	}

	agent, err := buildAgent(name, f)
	if err != nil {
		return err
	}

	// Show preview.
	pterm.Println()
	pterm.DefaultSection.Println("Create LedgerClusterAgent Preview")
	previewRows := [][]string{
		{"Name", pterm.Cyan(name)},
		{"Scope", "Cluster"},
		{"Scopes", strings.Join(f.scopes, ", ")},
		{"Selector", f.selector},
	}
	cmdutil.RenderBoxedTable(previewRows)
	pterm.Println()

	if f.dryRun {
		b, err := yaml.Marshal(agent)
		if err != nil {
			return fmt.Errorf("marshaling YAML: %w", err)
		}
		pterm.Info.Println("Dry run - YAML output:")
		pterm.Println()
		fmt.Print(string(b))

		return nil
	}

	confirm, err := cmdutil.PromptConfirm("Create this LedgerClusterAgent?", true)
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

	spinner, _ := pterm.DefaultSpinner.Start("Creating LedgerClusterAgent...")

	if err := crdClient.Create(cmd.Context(), agent); err != nil {
		spinner.Fail("Failed to create LedgerClusterAgent")

		return fmt.Errorf("creating agent %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("LedgerClusterAgent %s created", pterm.Cyan(name)))

	return nil
}

func runCreateWizard(cmd *cobra.Command, f *createFlags) error {
	if !cmd.Flags().Changed("scopes") {
		input, err := cmdutil.PromptText("Scopes (comma-separated, e.g. write,read)", "")
		if err != nil {
			return err
		}
		if input != "" {
			f.scopes = splitAndTrim(input)
		}
	}

	if !cmd.Flags().Changed("selector") {
		input, err := cmdutil.PromptText("Label selector (key=value,...)", "")
		if err != nil {
			return err
		}
		f.selector = input
	}

	return nil
}

func resolveAgentName(args []string) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}

	name, err := cmdutil.PromptText("LedgerClusterAgent name", "")
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", errors.New("name is required")
	}

	return name, nil
}

func buildAgent(name string, f *createFlags) (*ledgerv1alpha1.LedgerClusterAgent, error) {
	agent := &ledgerv1alpha1.LedgerClusterAgent{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "LedgerClusterAgent",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: ledgerv1alpha1.LedgerClusterAgentSpec{
			Scopes: f.scopes,
		},
	}

	if f.selector != "" {
		matchLabels, err := parseSelectorString(f.selector)
		if err != nil {
			return nil, err
		}
		agent.Spec.Selector = metav1.LabelSelector{
			MatchLabels: matchLabels,
		}
	}

	return agent, nil
}

// parseSelectorString parses "key=value,key2=value2" into a map.
func parseSelectorString(s string) (map[string]string, error) {
	result := make(map[string]string)
	for _, pair := range splitAndTrim(s) {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid selector pair %q, expected key=value", pair)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("empty key in selector pair %q", pair)
		}
		result[key] = value
	}

	return result, nil
}

// splitAndTrim splits a comma-separated string and trims whitespace from each element.
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}

	return result
}
