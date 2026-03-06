package agents

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type getKeyFlags struct {
	outputDir string
	seedOnly  bool
	bundle    string
}

func newGetKeyCommand(opts *cmdutil.Options) *cobra.Command {
	var f getKeyFlags

	cmd := &cobra.Command{
		Use:   "get-key [name]",
		Short: "Retrieve the Ed25519 key material from an agent's Secret",
		Long: `Retrieves the Ed25519 keypair (seed and public key) from the Secret
associated with a LedgerClusterAgent. By default, displays key-id, public key,
and seed in a formatted table.

Use --output-dir to write seed.hex and pubkey.hex files to a directory.
Use --seed-only to print just the seed hex to stdout (useful for piping).
Use --bundle - to output a JSON key bundle to stdout (for piping to ledgerctl auth login).
Use --bundle <path> to write the bundle to a file.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGetKey(cmd, opts, &f, args)
		},
	}

	cmd.Flags().StringVarP(&f.outputDir, "output-dir", "d", "", "Write seed.hex and pubkey.hex to this directory")
	cmd.Flags().BoolVar(&f.seedOnly, "seed-only", false, "Print only the seed hex to stdout")
	cmd.Flags().StringVar(&f.bundle, "bundle", "", "Output a JSON key bundle (use - for stdout, or a file path)")
	cmd.MarkFlagsMutuallyExclusive("output-dir", "seed-only", "bundle")

	return cmd
}

func runGetKey(cmd *cobra.Command, opts *cmdutil.Options, f *getKeyFlags, args []string) error {
	ctx := cmd.Context()

	name, err := cmdutil.ResolveLedgerClusterAgentName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	agent, err := cmdutil.GetLedgerClusterAgent(ctx, crdClient, name)
	if err != nil {
		return fmt.Errorf("getting agent %q: %w", name, err)
	}

	if agent.Status.SecretRef.Name == "" {
		return fmt.Errorf("agent %q does not have a secret reference yet (phase: %s)", name, agent.Status.Phase)
	}

	// Read the Secret.
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: agent.Status.SecretRef.Namespace,
		Name:      agent.Status.SecretRef.Name,
	}
	if err := crdClient.Get(ctx, secretKey, secret); err != nil {
		return fmt.Errorf("fetching secret %s/%s: %w", secretKey.Namespace, secretKey.Name, err)
	}

	seedHex := string(secret.Data["seed.hex"])
	pubKeyHex := string(secret.Data["pubkey.hex"])
	keyID := string(secret.Data["key-id"])

	if f.seedOnly {
		fmt.Print(seedHex)

		return nil
	}

	if f.bundle != "" {
		b := struct {
			SigningKey string   `json:"signingKey"`
			KeyID      string   `json:"keyId"`
			Scopes     []string `json:"scopes"`
			Subject    string   `json:"subject"`
		}{
			SigningKey: seedHex,
			KeyID:      keyID,
			Scopes:     agent.Spec.Scopes,
			Subject:    name,
		}

		if f.bundle == "-" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")

			return enc.Encode(b)
		}

		file, err := os.OpenFile(f.bundle, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return fmt.Errorf("creating bundle file: %w", err)
		}

		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")
		if err := enc.Encode(b); err != nil {
			_ = file.Close() // best-effort close on encode error

			return fmt.Errorf("writing bundle: %w", err)
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("closing bundle file: %w", err)
		}

		pterm.Success.Printfln("Bundle written to %s", f.bundle)

		return nil
	}

	if f.outputDir != "" {
		if err := os.MkdirAll(f.outputDir, 0o700); err != nil {
			return fmt.Errorf("creating output directory: %w", err)
		}

		seedPath := filepath.Join(f.outputDir, "seed.hex")
		if err := os.WriteFile(seedPath, []byte(seedHex), 0o600); err != nil {
			return fmt.Errorf("writing seed.hex: %w", err)
		}

		pubKeyPath := filepath.Join(f.outputDir, "pubkey.hex")
		if err := os.WriteFile(pubKeyPath, []byte(pubKeyHex), 0o644); err != nil {
			return fmt.Errorf("writing pubkey.hex: %w", err)
		}

		pterm.Success.Printfln("Keys written to %s", f.outputDir)
		pterm.Info.Printfln("  seed.hex:   %s (mode 0600)", seedPath)
		pterm.Info.Printfln("  pubkey.hex: %s (mode 0644)", pubKeyPath)

		return nil
	}

	// Default: show formatted table.
	pterm.Println()
	pterm.DefaultSection.Printfln("Agent Key: %s", pterm.Cyan(name))
	cmdutil.RenderBoxedTable([][]string{
		{"Key ID", keyID},
		{"Public Key", pubKeyHex},
		{"Seed", pterm.Yellow(seedHex)},
		{"Secret", fmt.Sprintf("%s/%s", secretKey.Namespace, secretKey.Name)},
	})
	pterm.Println()
	pterm.Warning.Println("Keep the seed value secret! It is the Ed25519 private key material.")

	return nil
}
