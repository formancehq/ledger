package main

import (
	"github.com/formancehq/go-libs/v3/service"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"
)

// Version information (set at build time)
var (
	version = "dev"
)

func main() {
	service.Execute(newRootCommand())
}

// newRootCommand creates the root command for the ledger client CLI.
func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "ledgerctl",
		Short:        "Ledger v3 CLI client",
		Long:         "Command-line client for interacting with Ledger v3 servers via gRPC",
		SilenceUsage: true,
	}

	// Add persistent flags for server connection
	rootCmd.PersistentFlags().String("server", "localhost:8888", "gRPC server address")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS)")
	rootCmd.PersistentFlags().String("tls-ca-cert", "", "Path to CA certificate file (PEM) for server verification")

	// Add persistent flags for request signing
	rootCmd.PersistentFlags().String("signing-key", "", "Path to Ed25519 private key file (seed: 32 bytes raw or hex-encoded)")
	rootCmd.PersistentFlags().String("signing-key-id", "", "Key ID for request signatures (default: \"default\")")

	// Add subcommands
	rootCmd.AddCommand(newLedgersCommand())
	rootCmd.AddCommand(newAccountsCommand())
	rootCmd.AddCommand(newTransactionsCommand())
	rootCmd.AddCommand(newStoreCommand())
	rootCmd.AddCommand(newClusterCommand())
	rootCmd.AddCommand(newAuditCommand())
	rootCmd.AddCommand(newSigningCommand())
	rootCmd.AddCommand(newEventsCommand())
	rootCmd.AddCommand(newPeriodsCommand())
	rootCmd.AddCommand(newVersionCommand())

	return rootCmd
}

// newVersionCommand creates the version command.
func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			// Display banner
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
