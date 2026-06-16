package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounttypes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/audit"
	authcmd "github.com/formancehq/ledger/v3/cmd/ledgerctl/auth"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cluster"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/events"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/logs"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/numscripts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/periods"
	profilecmd "github.com/formancehq/ledger/v3/cmd/ledgerctl/profile"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/provision"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/queries"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/querycheckpoint"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/restore"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/signing"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/store"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/upgrade"
)

// Version information (set at build time).
var version = "dev"

func main() {
	rootCmd := newRootCommand()
	rootCmd.SilenceErrors = true
	service.BindEnvToCommand(rootCmd)

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
			profileName, _ := cmd.Flags().GetString("profile")
			profileExplicit := cmd.Flags().Changed("profile")

			if profileName == "" {
				if v, ok := os.LookupEnv("LEDGERCTL_PROFILE"); ok && v != "" {
					profileName = strings.TrimSpace(v)
					profileExplicit = true
				}
			}

			name, p := cmdutil.GetActiveProfile(cfg, profileName)
			if profileExplicit && p == nil && !isProfileBootstrapCommand(cmd) {
				return fmt.Errorf("profile %q not found", name)
			}

			// Resolve flags: explicit CLI flag > env var > profile value > cobra default.
			resolveFlag(cmd, "server", "SERVER", cmdutil.ProfileFlagValue(p, "server"))
			resolveFlag(cmd, "insecure", "INSECURE", cmdutil.ProfileFlagValue(p, "insecure"))
			resolveFlag(cmd, "tls-ca-cert", "TLS_CA_CERT", cmdutil.ProfileFlagValue(p, "tls-ca-cert"))
			resolveFlag(cmd, "consistency", "CONSISTENCY", "")
			resolveFlag(cmd, "auth-token", "AUTH_TOKEN", "")
			resolveFlag(cmd, "signing-key", "SIGNING_KEY", cmdutil.ProfileFlagValue(p, "signing-key"))
			resolveFlag(cmd, "signing-key-id", "SIGNING_KEY_ID", cmdutil.ProfileFlagValue(p, "signing-key-id"))
			resolveFlag(cmd, "response-verify-key", "RESPONSE_VERIFY_KEY", cmdutil.ProfileFlagValue(p, "response-verify-key"))

			return nil
		},
	}

	// Add persistent flags for connection profiles.
	rootCmd.PersistentFlags().String("profile", "", "Connection profile name (env: LEDGERCTL_PROFILE)")

	// Add persistent flags for server connection.
	rootCmd.PersistentFlags().String("server", "localhost:8888", "gRPC server address (env: SERVER)")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS) (env: INSECURE)")
	rootCmd.PersistentFlags().String("tls-ca-cert", "", "Path to CA certificate file (PEM) for server verification (env: TLS_CA_CERT)")

	// Add persistent flags for request signing.
	rootCmd.PersistentFlags().String("signing-key", "", "Path to Ed25519 private key file (seed: 32 bytes raw or hex-encoded) (env: SIGNING_KEY)")
	rootCmd.PersistentFlags().String("signing-key-id", "", "Key ID for request signatures (default: \"default\") (env: SIGNING_KEY_ID)")

	// Add persistent flag for response signature verification.
	rootCmd.PersistentFlags().String("response-verify-key", "", "Path to Ed25519 seed file for verifying server response signatures (env: RESPONSE_VERIFY_KEY)")

	// Add persistent flag for read consistency level.
	rootCmd.PersistentFlags().String("consistency", "", "Read consistency level: stale, leader, or linearizable (default) (env: CONSISTENCY)")

	// Add persistent flag for bearer token authentication.
	rootCmd.PersistentFlags().String("auth-token", "", "Bearer token for authentication (JWT string or @path-to-file) (env: AUTH_TOKEN)")

	// Add persistent flag for an out-of-band sink of the --json result.
	// Generic on purpose: a CI wrapper, automation script, or the
	// ledger-operator (which points it at /dev/termination-log so the
	// kubelet captures the result on pod.status) can all opt in.
	rootCmd.PersistentFlags().String("result-file", "", "Also write the --json result to this file path (env: RESULT_FILE)")

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
	rootCmd.AddCommand(periods.NewCommand())
	rootCmd.AddCommand(restore.NewCommand())
	rootCmd.AddCommand(authcmd.NewCommand())
	rootCmd.AddCommand(profilecmd.NewCommand())
	rootCmd.AddCommand(newVersionCommand())
	rootCmd.AddCommand(upgrade.NewCommand(version))
	rootCmd.AddCommand(provision.NewCommand())
	rootCmd.AddCommand(querycheckpoint.NewCommand())
	rootCmd.AddCommand(queries.NewCommand())
	rootCmd.AddCommand(numscripts.NewCommand())

	return rootCmd
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
				Println(pterm.Sprintf("%s %s", pterm.LightCyan("Version:"), pterm.Green(version)))
		},
	}
}
