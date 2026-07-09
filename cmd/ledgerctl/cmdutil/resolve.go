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
	// *active* profile, writing that profile's values onto the shared persistent
	// flags before the real per-completion resolution (driven by the explicit
	// --profile in the args) runs. The connection-completion functions call
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
	profileName, profileExplicit := ResolveProfileName(cmd)

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

	// Deliberately no resolveFlag on --key-id here: pre-populating --key-id
	// from profile.signingKeyId would clobber a bare KEY_ID env value already
	// applied by bindSubcommandEnv (both are Changed=false, so we can't tell
	// the two sources apart at this layer). Instead, auth's `resolveKeyID`
	// reads --signing-key-id as the sibling fallback — that flag receives the
	// profile value via the resolveFlag call above, and its precedence chain
	// is CLI > LEDGERCTL_SIGNING_KEY_ID > profile.

	return nil
}

// resolveFlag sets a cobra flag's value using the first available source:
// explicit CLI flag > environment variable > profile value > cobra default.
// It only writes to the flag when it was not explicitly set on the command line.
//
// Env- and profile-derived values are applied through Flag.Value.Set instead of
// FlagSet.Set so the flag's Changed bit stays false: Changed must keep meaning
// "the user typed this on the CLI". auth login and cmdutil.ResolveTokenSource
// both read Changed to distinguish CLI-passed from env/profile-derived values
// — a Set() call here would light Changed even for env-only inputs and quietly
// break both callers (e.g. LEDGERCTL_SERVER would silently overwrite the active
// profile's server address on `auth login`).
func resolveFlag(cmd *cobra.Command, flagName, envVar, profileValue string) {
	if cmd.Flags().Changed(flagName) {
		return
	}

	var value string

	switch {
	case envVar != "" && os.Getenv(envVar) != "":
		value = strings.TrimSpace(os.Getenv(envVar))
	case profileValue != "":
		value = profileValue
	default:
		return
	}

	f := cmd.Flags().Lookup(flagName)
	if f == nil {
		return
	}

	// Value.Set updates the underlying value without touching Flag.Changed.
	_ = f.Value.Set(value)
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
