package cmdutil

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// ResolveConnectionFlags applies the profile/env/flag precedence to ledgerctl's
// connection and security flags, in order: explicit CLI flag > LEDGERCTL_ env
// var > active profile value > cobra default.
//
// It runs from the root PersistentPreRunE for normal command execution, and is
// ALSO invoked explicitly from shell-completion functions (the --ledger flag
// completion and the prepared-query ValidArgsFunction). cobra does NOT run
// PersistentPreRunE during `__complete`, so without this call completion would
// connect to the default --server (localhost:8888) instead of the one selected
// by --profile, returning ledgers from the wrong cluster. EN-1295.
func ResolveConnectionFlags(cmd *cobra.Command) error {
	// During `__complete`, cobra runs the root PersistentPreRunE with cmd set to
	// the completion helper command, whose args are the line being completed — so
	// --profile is NOT parsed onto it and resolution would fall back to the
	// *active* profile. Worse, resolveFlag writes that active profile's values
	// onto the shared persistent flags and marks them Changed, which then blocks
	// the real per-completion resolution (driven by the explicit --profile in the
	// args) from overriding them. The connection-completion functions call
	// ResolveConnectionFlags themselves with the real target command, so this
	// pre-run pass during completion must be a no-op. EN-1295.
	if cmd.Name() == cobra.ShellCompRequestCmd || cmd.Name() == cobra.ShellCompNoDescRequestCmd {
		return nil
	}

	// Skip profile/env resolution for profile management commands — they define
	// local flags with the same names and must not be contaminated by the active
	// profile or environment variables.
	if isProfileCommand(cmd) {
		return nil
	}

	// Load config and resolve the active profile.
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}

	// Resolve profile name: --profile flag > LEDGERCTL_PROFILE env > config activeProfile.
	profileName, _ := cmd.Flags().GetString("profile")
	profileExplicit := cmd.Flags().Changed("profile")

	if profileName == "" {
		if v, ok := os.LookupEnv("LEDGERCTL_PROFILE"); ok && v != "" {
			profileName = strings.TrimSpace(v)
			profileExplicit = true
		}
	}

	name, p := GetActiveProfile(cfg, profileName)
	if profileExplicit && p == nil && !isProfileBootstrapCommand(cmd) {
		return fmt.Errorf("profile %q not found", name)
	}

	// Resolve flags: explicit CLI flag > env var > profile value > cobra default.
	resolveFlag(cmd, "server", "LEDGERCTL_SERVER", ProfileFlagValue(p, "server"))
	resolveFlag(cmd, "insecure", "LEDGERCTL_INSECURE", ProfileFlagValue(p, "insecure"))
	resolveFlag(cmd, "tls-ca-cert", "LEDGERCTL_TLS_CA_CERT", ProfileFlagValue(p, "tls-ca-cert"))
	resolveFlag(cmd, "consistency", "LEDGERCTL_CONSISTENCY", "")
	resolveFlag(cmd, "auth-token", "LEDGERCTL_AUTH_TOKEN", "")
	resolveFlag(cmd, "signing-key", "LEDGERCTL_SIGNING_KEY", ProfileFlagValue(p, "signing-key"))
	resolveFlag(cmd, "signing-key-id", "LEDGERCTL_SIGNING_KEY_ID", ProfileFlagValue(p, "signing-key-id"))
	resolveFlag(cmd, "response-verify-key", "LEDGERCTL_RESPONSE_VERIFY_KEY", ProfileFlagValue(p, "response-verify-key"))
	resolveFlag(cmd, "result-file", "LEDGERCTL_RESULT_FILE", "")

	return nil
}

// resolveFlag sets a cobra flag's value using the first available source:
// explicit CLI flag > environment variable > profile value > cobra default.
// It only writes to the flag when it was not explicitly set on the command line.
func resolveFlag(cmd *cobra.Command, flagName, envVar, profileValue string) {
	if cmd.Flags().Changed(flagName) {
		return
	}

	if v, ok := os.LookupEnv(envVar); ok && v != "" {
		_ = cmd.Flags().Set(flagName, strings.TrimSpace(v))

		return
	}

	if profileValue != "" {
		_ = cmd.Flags().Set(flagName, profileValue)
	}
}

// isProfileCommand returns true when cmd is a subcommand of "profile".
// Profile management commands define local flags that overlap with the
// persistent connection flags (--server, --insecure, --tls-ca-cert) and
// must not inherit values from the active profile or environment.
func isProfileCommand(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		if c.Name() == "profile" {
			return true
		}
	}

	return false
}

// isProfileBootstrapCommand returns true for commands that should work even
// when the referenced --profile does not exist yet (e.g. auth login, profile create).
func isProfileBootstrapCommand(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		switch c.Name() {
		case "login", "create":
			if p := c.Parent(); p != nil && (p.Name() == "auth" || p.Name() == "profile") {
				return true
			}
		}
	}

	return false
}
