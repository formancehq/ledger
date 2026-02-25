package cmdutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
)

// ResolveLedgerName returns the ledger name from args or by interactive selection.
// If args contains a name, it is returned directly. Otherwise, lists ledgers in
// the resolved namespace and prompts the user to select one.
func ResolveLedgerName(ctx context.Context, opts *Options, args []string) (name, namespace string, err error) {
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

	spinner, _ := pterm.DefaultSpinner.Start("Fetching Ledger resources...")

	ledgers, err := ListLedgers(ctx, crdClient, ns)
	if err != nil {
		spinner.Fail("Failed to list Ledger resources")
		return "", "", fmt.Errorf("listing ledgers: %w", err)
	}

	_ = spinner.Stop()

	if len(ledgers.Items) == 0 {
		return "", "", fmt.Errorf("no Ledger resources found in namespace %q", ns)
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
		WithDefaultText("Select a Ledger").
		Show()
	if err != nil {
		return "", "", fmt.Errorf("failed to select ledger: %w", err)
	}

	return selected, ns, nil
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
	defaultStr := fmt.Sprintf("%d", defaultValue)

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
