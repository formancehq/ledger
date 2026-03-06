package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"

	"github.com/formancehq/go-libs/v3/service"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/audit"
	authcmd "github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/auth"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cluster"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/events"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/logs"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/periods"
	profilecmd "github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/profile"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/restore"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/signing"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/store"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/upgrade"
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
			if profileExplicit && p == nil {
				return fmt.Errorf("profile %q not found", name)
			}

			// Resolve flags: explicit CLI flag > env var > profile value > cobra default.
			resolveFlag(cmd, "server", "SERVER", cmdutil.ProfileFlagValue(p, "server"))
			resolveFlag(cmd, "insecure", "INSECURE", cmdutil.ProfileFlagValue(p, "insecure"))
			resolveFlag(cmd, "tls-ca-cert", "TLS_CA_CERT", cmdutil.ProfileFlagValue(p, "tls-ca-cert"))
			resolveFlag(cmd, "consistency", "CONSISTENCY", "")
			resolveFlag(cmd, "auth-token", "AUTH_TOKEN", "")

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
	rootCmd.PersistentFlags().String("signing-key", "", "Path to Ed25519 private key file (seed: 32 bytes raw or hex-encoded)")
	rootCmd.PersistentFlags().String("signing-key-id", "", "Key ID for request signatures (default: \"default\")")

	// Add persistent flag for response signature verification.
	rootCmd.PersistentFlags().String("response-verify-key", "", "Path to Ed25519 seed file for verifying server response signatures")

	// Add persistent flag for read consistency level.
	rootCmd.PersistentFlags().String("consistency", "", "Read consistency level: stale, leader, or linearizable (default) (env: CONSISTENCY)")

	// Add persistent flag for bearer token authentication.
	rootCmd.PersistentFlags().String("auth-token", "", "Bearer token for authentication (JWT string or @path-to-file) (env: AUTH_TOKEN)")

	// Add subcommands.
	rootCmd.AddCommand(ledgers.NewCommand())
	rootCmd.AddCommand(accounts.NewCommand())
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

	return rootCmd
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

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
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
