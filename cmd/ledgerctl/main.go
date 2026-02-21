package main

import (
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/audit"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cluster"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/events"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/logs"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/periods"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/restore"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/signing"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/store"
	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/transactions"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"
)

// Version information (set at build time).
var version = "dev"

func main() {
	service.Execute(newRootCommand())
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "ledgerctl",
		Short:        "Ledger v3 CLI client",
		Long:         "Command-line client for interacting with Ledger v3 servers via gRPC",
		SilenceUsage: true,
	}

	// Add persistent flags for server connection.
	rootCmd.PersistentFlags().String("server", "localhost:8888", "gRPC server address")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS)")
	rootCmd.PersistentFlags().String("tls-ca-cert", "", "Path to CA certificate file (PEM) for server verification")

	// Add persistent flags for request signing.
	rootCmd.PersistentFlags().String("signing-key", "", "Path to Ed25519 private key file (seed: 32 bytes raw or hex-encoded)")
	rootCmd.PersistentFlags().String("signing-key-id", "", "Key ID for request signatures (default: \"default\")")

	// Add persistent flag for response signature verification.
	rootCmd.PersistentFlags().String("response-verify-key", "", "Path to Ed25519 seed file for verifying server response signatures")

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
	rootCmd.AddCommand(newVersionCommand())

	return rootCmd
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
