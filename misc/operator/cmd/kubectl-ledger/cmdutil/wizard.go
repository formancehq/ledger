package cmdutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResolveClusterName returns the ledger name from args or by interactive selection.
// If args contains a name, it is returned directly. Otherwise, lists ledgers in
// the resolved namespace and prompts the user to select one.
func ResolveClusterName(ctx context.Context, opts *Options, args []string) (name, namespace string, err error) {
	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return "", "", fmt.Errorf("resolving namespace: %w", err)
	}
	name, err = resolveResourceName(opts, args, "Cluster", "ledger", ns, func(c client.Client) ([]string, error) {
		list, err := ListClusters(ctx, c, ns)
		if err != nil {
			return nil, err
		}
		names := make([]string, len(list.Items))
		for index := range list.Items {
			names[index] = list.Items[index].Name
		}

		return names, nil
	})
	if err != nil {
		return "", "", err
	}

	return name, ns, nil
}

// ResolveBackupName returns the Backup name from args or by interactive selection.
func ResolveBackupName(ctx context.Context, opts *Options, args []string) (name, namespace string, err error) {
	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return "", "", fmt.Errorf("resolving namespace: %w", err)
	}
	name, err = resolveResourceName(opts, args, "Backup", "backup", ns, func(c client.Client) ([]string, error) {
		list, err := ListBackups(ctx, c, ns)
		if err != nil {
			return nil, err
		}
		names := make([]string, len(list.Items))
		for index := range list.Items {
			names[index] = list.Items[index].Name
		}

		return names, nil
	})
	if err != nil {
		return "", "", err
	}

	return name, ns, nil
}

// ResolveCredentialsName returns the Credentials name from args
// or by interactive selection. Credentials is cluster-scoped so no namespace needed.
func ResolveCredentialsName(ctx context.Context, opts *Options, args []string) (string, error) {
	return resolveResourceName(opts, args, "Credentials", "credentials", "", func(c client.Client) ([]string, error) {
		list, err := ListCredentials(ctx, c)
		if err != nil {
			return nil, err
		}
		names := make([]string, len(list.Items))
		for index := range list.Items {
			names[index] = list.Items[index].Name
		}

		return names, nil
	})
}

func resolveResourceName(opts *Options, args []string, kind, noun, namespace string, listNames func(client.Client) ([]string, error)) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}
	crdClient, err := opts.CRDClient()
	if err != nil {
		return "", fmt.Errorf("creating client: %w", err)
	}
	structured := opts.OutputFormat() == "json" || opts.OutputFormat() == "yaml"
	if !structured {
		pterm.Info.Printfln("Fetching %s resources...", kind)
	}
	names, err := listNames(crdClient)
	if err != nil {
		pterm.Error.Printfln("Failed to list %s resources", kind)
		plural := noun + "s"
		if kind == "Credentials" {
			plural = noun
		}

		return "", fmt.Errorf("listing %s: %w", plural, err)
	}
	if len(names) == 0 {
		if namespace == "" {
			return "", fmt.Errorf("no %s resources found", kind)
		}

		return "", fmt.Errorf("no %s resources found in namespace %q", kind, namespace)
	}
	if len(names) == 1 {
		if !structured {
			pterm.Info.Printfln("Using %s: %s", noun, pterm.Cyan(names[0]))
		}

		return names[0], nil
	}
	if structured {
		return "", fmt.Errorf("multiple %s resources found: specify an explicit %s name with -o %s", kind, noun, opts.OutputFormat())
	}
	prompt := "Select a " + kind
	var selected string
	if opts.selectResource != nil {
		selected, err = opts.selectResource(prompt, names)
	} else {
		selected, err = pterm.DefaultInteractiveSelect.WithOptions(names).WithDefaultText(prompt).Show()
	}
	if err != nil {
		return "", fmt.Errorf("failed to select %s: %w", noun, err)
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
