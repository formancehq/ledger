package profile

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewCreateCommand returns the "profile create" command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new connection profile",
		Long: `Create a named connection profile with server address and TLS settings.

If this is the first profile, it is automatically set as the active profile.
Use --use to activate it immediately even when other profiles already exist.`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runCreate,
	}

	cmd.Flags().String("server", "", "gRPC server address (required)")
	cmd.Flags().Bool("insecure", false, "Use insecure connection (no TLS)")
	cmd.Flags().String("tls-ca-cert", "", "Path to CA certificate file (PEM)")
	cmd.Flags().String("tls-server-name", "", "Hostname to verify against the server certificate SANs, overriding the --server host")
	cmd.Flags().String("signing-key", "", "Path to Ed25519 private key file for request signing")
	cmd.Flags().String("signing-key-id", "", "Key ID for request signatures")
	cmd.Flags().String("response-verify-key", "", "Path to Ed25519 seed file for verifying server response signatures")
	cmd.Flags().Bool("use", false, "Set this profile as the active profile")
	_ = cmd.MarkFlagRequired("server")

	return cmd
}

func runCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	cfg, err := cmdutil.LoadConfig()
	if err != nil {
		return err
	}

	if cfg.Profiles != nil {
		if _, exists := cfg.Profiles[name]; exists {
			return fmt.Errorf("profile %q already exists", name)
		}
	}

	server, _ := cmd.Flags().GetString("server")
	insecure, _ := cmd.Flags().GetBool("insecure")
	tlsCaCert, _ := cmd.Flags().GetString("tls-ca-cert")
	tlsServerName, _ := cmd.Flags().GetString("tls-server-name")
	signingKey, _ := cmd.Flags().GetString("signing-key")
	signingKeyID, _ := cmd.Flags().GetString("signing-key-id")
	responseVerifyKey, _ := cmd.Flags().GetString("response-verify-key")
	useFlag, _ := cmd.Flags().GetBool("use")

	p := cmdutil.Profile{
		Server:            server,
		Insecure:          insecure,
		TLSCaCert:         tlsCaCert,
		TLSServerName:     tlsServerName,
		SigningKey:        signingKey,
		SigningKeyID:      signingKeyID,
		ResponseVerifyKey: responseVerifyKey,
	}

	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]cmdutil.Profile)
	}

	cfg.Profiles[name] = p

	// Auto-activate if first profile or --use was given.
	if len(cfg.Profiles) == 1 || useFlag {
		cfg.ActiveProfile = name
	}

	if err := cmdutil.SaveConfig(cfg); err != nil {
		return err
	}

	pterm.Success.Printfln("Profile %s created (server: %s)", pterm.Bold.Sprint(name), server)

	if cfg.ActiveProfile == name {
		pterm.Info.Printfln("Active profile: %s", pterm.Bold.Sprint(name))
	}

	return nil
}
