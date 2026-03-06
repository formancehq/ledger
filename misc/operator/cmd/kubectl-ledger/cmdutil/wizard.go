package cmdutil

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
)

// ResolveLedgerServiceName returns the ledger name from args or by interactive selection.
// If args contains a name, it is returned directly. Otherwise, lists ledgers in
// the resolved namespace and prompts the user to select one.
func ResolveLedgerServiceName(ctx context.Context, opts *Options, args []string) (name, namespace string, err error) {
	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return "", "", fmt.Errorf("resolving namespace: %w", err)
	}

	if len(args) > 0 {
		return args[0], ns, nil
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return "", "", fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching LedgerService resources...")

	ledgers, err := ListLedgerServices(ctx, crdClient, ns)
	if err != nil {
		spinner.Fail("Failed to list LedgerService resources")

		return "", "", fmt.Errorf("listing ledgers: %w", err)
	}

	_ = spinner.Stop()

	if len(ledgers.Items) == 0 {
		return "", "", fmt.Errorf("no LedgerService resources found in namespace %q", ns)
	}

	names := make([]string, len(ledgers.Items))
	for i := range ledgers.Items {
		names[i] = ledgers.Items[i].Name
	}

	if len(names) == 1 {
		pterm.Info.Printfln("Using ledger: %s", pterm.Cyan(names[0]))

		return names[0], ns, nil
	}

	selected, err := pterm.DefaultInteractiveSelect.
		WithOptions(names).
		WithDefaultText("Select a LedgerService").
		Show()
	if err != nil {
		return "", "", fmt.Errorf("failed to select ledger: %w", err)
	}

	return selected, ns, nil
}

// ResolveLedgerDefaultsName returns the LedgerDefaults name from args or by
// interactive selection. LedgerDefaults is cluster-scoped so no namespace needed.
func ResolveLedgerDefaultsName(ctx context.Context, opts *Options, args []string) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return "", fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching LedgerDefaults resources...")

	defaults, err := ListLedgerDefaults(ctx, crdClient)
	if err != nil {
		spinner.Fail("Failed to list LedgerDefaults resources")

		return "", fmt.Errorf("listing ledger defaults: %w", err)
	}

	_ = spinner.Stop()

	if len(defaults.Items) == 0 {
		return "", errors.New("no LedgerDefaults resources found")
	}

	names := make([]string, len(defaults.Items))
	for i := range defaults.Items {
		names[i] = defaults.Items[i].Name
	}

	if len(names) == 1 {
		pterm.Info.Printfln("Using defaults: %s", pterm.Cyan(names[0]))

		return names[0], nil
	}

	selected, err := pterm.DefaultInteractiveSelect.
		WithOptions(names).
		WithDefaultText("Select a LedgerDefaults").
		Show()
	if err != nil {
		return "", fmt.Errorf("failed to select defaults: %w", err)
	}

	return selected, nil
}

// ResolveLedgerClusterAgentName returns the LedgerClusterAgent name from args
// or by interactive selection. LedgerClusterAgent is cluster-scoped so no namespace needed.
func ResolveLedgerClusterAgentName(ctx context.Context, opts *Options, args []string) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return "", fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Fetching LedgerClusterAgent resources...")

	agents, err := ListLedgerClusterAgents(ctx, crdClient)
	if err != nil {
		spinner.Fail("Failed to list LedgerClusterAgent resources")

		return "", fmt.Errorf("listing agents: %w", err)
	}

	_ = spinner.Stop()

	if len(agents.Items) == 0 {
		return "", errors.New("no LedgerClusterAgent resources found")
	}

	names := make([]string, len(agents.Items))
	for i := range agents.Items {
		names[i] = agents.Items[i].Name
	}

	if len(names) == 1 {
		pterm.Info.Printfln("Using agent: %s", pterm.Cyan(names[0]))

		return names[0], nil
	}

	selected, err := pterm.DefaultInteractiveSelect.
		WithOptions(names).
		WithDefaultText("Select a LedgerClusterAgent").
		Show()
	if err != nil {
		return "", fmt.Errorf("failed to select agent: %w", err)
	}

	return selected, nil
}

// PromptText prompts the user for a text value with an optional default.
func PromptText(prompt, defaultValue string) (string, error) {
	input := pterm.DefaultInteractiveTextInput.WithDefaultText(prompt)
	if defaultValue != "" {
		input = input.WithDefaultValue(defaultValue)
	}
	result, err := input.Show()
	if err != nil {
		return "", fmt.Errorf("failed to read input: %w", err)
	}

	return strings.TrimSpace(result), nil
}

// MaxReplicas is the hard upper limit for Raft replicas.
const MaxReplicas = 7

// ValidReplicaCounts are the allowed replica values (odd, 1..MaxReplicas).
var ValidReplicaCounts = []string{"1", "3", "5", "7"}

// PromptReplicas prompts the user to select a replica count from valid options.
func PromptReplicas(defaultValue int32) (int32, error) {
	defaultStr := strconv.Itoa(int(defaultValue))

	selected, err := pterm.DefaultInteractiveSelect.
		WithOptions(ValidReplicaCounts).
		WithDefaultText("Number of replicas").
		WithDefaultOption(defaultStr).
		Show()
	if err != nil {
		return 0, fmt.Errorf("failed to select replicas: %w", err)
	}

	n, err := strconv.ParseInt(selected, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid replicas %q: %w", selected, err)
	}

	return int32(n), nil
}

// ValidateReplicas checks that a replica count is odd, >= 1, and <= MaxReplicas.
func ValidateReplicas(replicas int32) error {
	if replicas < 1 {
		return fmt.Errorf("replicas must be at least 1, got %d", replicas)
	}
	if replicas > MaxReplicas {
		return fmt.Errorf("replicas must be at most %d, got %d", MaxReplicas, replicas)
	}
	if replicas%2 == 0 {
		return fmt.Errorf("replicas must be odd for Raft consensus, got %d", replicas)
	}

	return nil
}

// PromptBool prompts the user to select a boolean value or unset it.
// Returns nil if the user selects "<unset>".
func PromptBool(prompt string, currentValue *bool) (*bool, error) {
	options := []string{"true", "false", "<unset>"}
	defaultOpt := "<unset>"
	if currentValue != nil {
		defaultOpt = strconv.FormatBool(*currentValue)
	}

	selected, err := pterm.DefaultInteractiveSelect.
		WithOptions(options).
		WithDefaultText(prompt).
		WithDefaultOption(defaultOpt).
		Show()
	if err != nil {
		return nil, fmt.Errorf("failed to select value: %w", err)
	}

	if selected == "<unset>" {
		return nil, nil //nolint:nilnil // nil means "unset"
	}
	v := selected == "true"

	return &v, nil
}

// PromptConfirm prompts the user for a yes/no confirmation.
func PromptConfirm(prompt string, defaultValue bool) (bool, error) {
	result, err := pterm.DefaultInteractiveConfirm.
		WithDefaultText(prompt).
		WithDefaultValue(defaultValue).
		Show()
	if err != nil {
		return false, fmt.Errorf("failed to read input: %w", err)
	}

	return result, nil
}
