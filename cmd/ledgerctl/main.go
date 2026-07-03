package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounttypes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/audit"
	authcmd "github.com/formancehq/ledger/v3/cmd/ledgerctl/auth"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/chapters"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cluster"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/events"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/logs"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/numscripts"
	profilecmd "github.com/formancehq/ledger/v3/cmd/ledgerctl/profile"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/provision"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/queries"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/querycheckpoint"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/restore"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/signing"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/store"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/upgrade"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

func main() {
	rootCmd := newRootCommand()
	rootCmd.SilenceErrors = true

	bindSubcommandEnv(rootCmd)

	err := rootCmd.Execute()
	if err != nil {
		var cliErr *cmdutil.CLIError
		if !errors.As(err, &cliErr) {
			// Error was not already displayed — print it now.
			pterm.Error.Println(err.Error())
		}

		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "ledgerctl",
		Short:        "Ledger v3 CLI client",
		Long:         "Command-line client for interacting with Ledger v3 servers via gRPC",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Keep stdout reserved for machine-readable payloads when the
			// caller asked for --json or --yaml. Spinners, success messages,
			// errors — every pterm printer — gets redirected to stderr so a
			// downstream `jq`, `yq`, or K8s log scraper sees only the encoded
			// result on stdout (and the termination-log mirror we drop in
			// EncodeStructured).
			cmdutil.RoutePtermForStructuredOutput(cmd)

			// Skip profile/env resolution for profile management commands —
			// they define local flags with the same names and must not be
			// contaminated by the active profile or environment variables.
			if isProfileCommand(cmd) {
				return nil
			}

			// Load config and resolve the active profile.
			cfg, err := cmdutil.LoadConfig()
			if err != nil {
				return err
			}

			// Resolve profile name: --profile flag > LEDGERCTL_PROFILE env > config activeProfile.
			profileName, profileExplicit := cmdutil.ResolveProfileName(cmd)

			name, p := cmdutil.GetActiveProfile(cfg, profileName)
			if profileExplicit && p == nil && !isProfileBootstrapCommand(cmd) {
				return fmt.Errorf("profile %q not found", name)
			}

			// Resolve flags: explicit CLI flag > env var > profile value > cobra default.
			resolveFlag(cmd, "server", "LEDGERCTL_SERVER", cmdutil.ProfileFlagValue(p, "server"))
			resolveFlag(cmd, "insecure", "LEDGERCTL_INSECURE", cmdutil.ProfileFlagValue(p, "insecure"))
			resolveFlag(cmd, "tls-ca-cert", "LEDGERCTL_TLS_CA_CERT", cmdutil.ProfileFlagValue(p, "tls-ca-cert"))
			resolveFlag(cmd, "consistency", "LEDGERCTL_CONSISTENCY", "")
			resolveFlag(cmd, "auth-token", "LEDGERCTL_AUTH_TOKEN", "")
			resolveFlag(cmd, "signing-key", "LEDGERCTL_SIGNING_KEY", cmdutil.ProfileFlagValue(p, "signing-key"))
			resolveFlag(cmd, "signing-key-id", "LEDGERCTL_SIGNING_KEY_ID", cmdutil.ProfileFlagValue(p, "signing-key-id"))
			resolveFlag(cmd, "response-verify-key", "LEDGERCTL_RESPONSE_VERIFY_KEY", cmdutil.ProfileFlagValue(p, "response-verify-key"))
			resolveFlag(cmd, "result-file", "LEDGERCTL_RESULT_FILE", "")

			// Deliberately no resolveFlag on --key-id here: pre-populating
			// --key-id from profile.signingKeyId would clobber a bare KEY_ID
			// env value already applied by bindSubcommandEnv (both are
			// Changed=false, so we can't tell the two sources apart at this
			// layer). Instead, auth's `resolveKeyID` reads --signing-key-id
			// as the sibling fallback — that flag receives the profile value
			// via the resolveFlag call above, and its precedence chain is
			// CLI > LEDGERCTL_SIGNING_KEY_ID > profile.

			return nil
		},
	}

	// Add persistent flags for connection profiles.
	rootCmd.PersistentFlags().String("profile", "", "Connection profile name (env: LEDGERCTL_PROFILE)")
	_ = rootCmd.RegisterFlagCompletionFunc("profile", cmdutil.CompleteProfileNames)

	// Add persistent flags for server connection.
	rootCmd.PersistentFlags().String("server", "localhost:8888", "gRPC server address (env: LEDGERCTL_SERVER)")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS) (env: LEDGERCTL_INSECURE)")
	rootCmd.PersistentFlags().String("tls-ca-cert", "", "Path to CA certificate file (PEM) for server verification (env: LEDGERCTL_TLS_CA_CERT)")

	// Add persistent flags for request signing.
	rootCmd.PersistentFlags().String("signing-key", "", "Path to Ed25519 private key file (seed: 32 bytes raw or hex-encoded) (env: LEDGERCTL_SIGNING_KEY)")
	rootCmd.PersistentFlags().String("signing-key-id", "", "Key ID for request signatures (default: \"default\") (env: LEDGERCTL_SIGNING_KEY_ID)")

	// Add persistent flag for response signature verification.
	rootCmd.PersistentFlags().String("response-verify-key", "", "Path to Ed25519 seed file for verifying server response signatures (env: LEDGERCTL_RESPONSE_VERIFY_KEY)")

	// Add persistent flag for read consistency level.
	rootCmd.PersistentFlags().String("consistency", "", "Read consistency level: stale, leader, or linearizable (default) (env: LEDGERCTL_CONSISTENCY)")
	cmdutil.RegisterEnumCompletion(rootCmd, "consistency", "stale", "leader", "linearizable")

	// Add persistent flag for bearer token authentication.
	rootCmd.PersistentFlags().String("auth-token", "", "Bearer token for authentication (JWT string or @path-to-file) (env: LEDGERCTL_AUTH_TOKEN)")

	// Add persistent flag for an out-of-band sink of the --json result.
	// Generic on purpose: a CI wrapper, automation script, or the
	// ledger-operator (which points it at /dev/termination-log so the
	// kubelet captures the result on pod.status) can all opt in.
	rootCmd.PersistentFlags().String("result-file", "", "Also write the --json result to this file path (env: LEDGERCTL_RESULT_FILE)")

	// Add subcommands.
	rootCmd.AddCommand(ledgers.NewCommand())
	rootCmd.AddCommand(indexes.NewCommand())
	rootCmd.AddCommand(accounts.NewCommand())
	rootCmd.AddCommand(accounttypes.NewCommand())
	rootCmd.AddCommand(transactions.NewCommand())
	rootCmd.AddCommand(store.NewCommand())
	rootCmd.AddCommand(cluster.NewCommand())
	rootCmd.AddCommand(audit.NewCommand())
	rootCmd.AddCommand(logs.NewCommand())
	rootCmd.AddCommand(signing.NewCommand())
	rootCmd.AddCommand(events.NewCommand())
	rootCmd.AddCommand(chapters.NewCommand())
	rootCmd.AddCommand(restore.NewCommand())
	rootCmd.AddCommand(authcmd.NewCommand())
	rootCmd.AddCommand(profilecmd.NewCommand())
	rootCmd.AddCommand(newVersionCommand())
	rootCmd.AddCommand(upgrade.NewCommand(version.Get().Version))
	rootCmd.AddCommand(provision.NewCommand())
	rootCmd.AddCommand(querycheckpoint.NewCommand())
	rootCmd.AddCommand(queries.NewCommand())
	rootCmd.AddCommand(numscripts.NewCommand())

	// Wire shell completion for every --ledger flag in the tree.
	registerLedgerFlagCompletion(rootCmd)

	return rootCmd
}

// registerLedgerFlagCompletion walks the command tree and attaches the
// ledger-name shell completion to every command exposing a --ledger flag
// (whether declared locally or inherited as a persistent flag).
//
// cobra keys completion functions by the flag pointer, so a command that only
// inherits the flag resolves to the same pointer as its declaring parent: the
// duplicate registration returns an "already registered" error, which we
// intentionally ignore. Registering once per declaring command is therefore
// sufficient and idempotent.
func registerLedgerFlagCompletion(cmd *cobra.Command) {
	if cmd.Flag("ledger") != nil {
		_ = cmd.RegisterFlagCompletionFunc("ledger", cmdutil.CompleteLedgerNames)
	}

	for _, sub := range cmd.Commands() {
		registerLedgerFlagCompletion(sub)
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

// ledgerctlOwnedFlagNames are the profile/connection/security flags ledgerctl
// resolves exclusively through the LEDGERCTL_ prefix (root PersistentPreRunE for
// inherited flags, ResolveTokenSource for the token). They must never be bound to
// bare go-libs env names anywhere in the tree — including the subcommands that
// redeclare them locally (profile create, auth generate-token) — or a stray
// PROFILE/SERVER/SIGNING_KEY/INSECURE/… silently overrides the prefixed lookup.
//
// profile is included even though cobra's lazy persistent-flag merge means a
// subcommand flag set never exposes the inherited --profile at bind time (so
// bare PROFILE is not reachable today): membership makes the documented
// "only LEDGERCTL_PROFILE" contract true by construction rather than by an
// accident of merge ordering, and it stays correct if a subcommand ever declares
// a local --profile. EN-1295.
var ledgerctlOwnedFlagNames = map[string]struct{}{
	"profile":             {},
	"server":              {},
	"insecure":            {},
	"tls-ca-cert":         {},
	"consistency":         {},
	"auth-token":          {},
	"signing-key":         {},
	"signing-key-id":      {},
	"response-verify-key": {},
	"result-file":         {},
}

// bindSubcommandEnv binds bare-name environment variables (the go-libs
// convention, e.g. KEY_ID -> --key-id) to every subcommand flag EXCEPT the
// ledgerctl-owned connection/security flags. The root command itself is never
// bound: its persistent flags are owned and resolved with the LEDGERCTL_ prefix
// in PersistentPreRunE. main() and TestServerFlagEnvResolution share this
// helper so the test exercises the production wiring rather than re-implementing it.
func bindSubcommandEnv(rootCmd *cobra.Command) {
	for _, sub := range rootCmd.Commands() {
		bindEnvSkippingOwned(sub)
	}
}

// bindEnvSkippingOwned mirrors service.BindEnvToCommand's recursion but skips
// the ledgerctl-owned flag names so their bare env aliases are never honored.
func bindEnvSkippingOwned(cmd *cobra.Command) {
	bindFlagSetSkippingOwned(cmd.Flags())
	bindFlagSetSkippingOwned(cmd.PersistentFlags())

	for _, sub := range cmd.Commands() {
		bindEnvSkippingOwned(sub)
	}
}

// bindFlagSetSkippingOwned binds each non-owned flag in set to its bare
// uppercased env name (matching service.BindEnvToFlagSet, including the
// stringSlice space-to-comma handling used by flags such as --scopes).
//
// Values are applied through Flag.Value.Set instead of FlagSet.Set so the
// flag's Changed bit stays false: Changed must mean "the user typed the flag
// on the CLI", not "we found a matching env var". auth login's resolveKeyID
// and its bundle-override guards rely on this — otherwise a stray KEY_ID
// env would beat an explicit CLI --signing-key-id, and env-derived
// key-id/subject/scopes would silently override bundle values that
// documentation says should win over env.
func bindFlagSetSkippingOwned(set *pflag.FlagSet) {
	set.VisitAll(func(flag *pflag.Flag) {
		if _, owned := ledgerctlOwnedFlagNames[flag.Name]; owned {
			return
		}

		envVar := strings.ReplaceAll(strings.ToUpper(flag.Name), "-", "_")

		value := os.Getenv(envVar)
		if value == "" {
			return
		}

		value = strings.TrimSpace(value)
		if flag.Value.Type() == "stringSlice" && strings.Contains(value, " ") {
			value = strings.ReplaceAll(value, " ", ",")
		}

		// Ignore the error: an invalid env value leaves the cobra default in
		// place, matching resolveFlag's best-effort env handling.
		_ = flag.Value.Set(value)
	})
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

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "version",
		Short:             "Print version information",
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		Run: func(_ *cobra.Command, _ []string) {
			banner, _ := pterm.DefaultBigText.WithLetters(
				putils.LettersFromStringWithStyle("Ledger", pterm.FgCyan.ToStyle()),
				putils.LettersFromStringWithStyle("ctl", pterm.FgLightMagenta.ToStyle()),
			).Srender()
			pterm.Println(banner)

			pterm.DefaultBox.WithTitle(pterm.LightGreen("Version Info")).
				Println(pterm.Sprintf("%s %s", pterm.LightCyan("Version:"), pterm.Green(version.Get().Version)))
		},
	}
}
