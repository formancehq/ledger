package auth

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/domain/crypto/signing"
)

// keyBundle is the JSON format for agent key bundles produced by
// `kubectl ledger agents get-key --bundle`.
type keyBundle struct {
	SigningKey string   `json:"signingKey"`
	KeyID      string   `json:"keyId"`
	Scopes     []string `json:"scopes"`
	Subject    string   `json:"subject"`
}

// NewLoginCommand returns the "auth login" command.
func NewLoginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Generate a token and store it in the OS keychain",
		Long: `Generate a signed EdDSA JWT token and store it in the OS keychain
(macOS Keychain, Linux libsecret, Windows Credential Manager) for the
current --server address.

Subsequent commands automatically use the stored token without --auth-token.

Accepts a JSON key bundle via --bundle or stdin pipe (produced by
kubectl ledger agents get-key --bundle). Explicit flags override bundle values.`,
		RunE: runLogin,
	}

	addTokenGenerationFlags(cmd)
	cmd.Flags().String("bundle", "", "Path to JSON key bundle file (or - for stdin)")

	return cmd
}

func runLogin(cmd *cobra.Command, _ []string) error {
	p, err := resolveLoginParams(cmd)
	if err != nil {
		return err
	}

	token, err := signToken(p)
	if err != nil {
		return err
	}

	server, _ := cmd.Flags().GetString("server")

	if err := cmdutil.GetKeyring(cmd).Set(server, token); err != nil {
		return fmt.Errorf("storing token in keychain: %w", err)
	}

	// If --server was explicitly passed and a profile is active, update the
	// profile's server address so that subsequent commands use the same address
	// (and find the keychain token keyed by the full address including port).
	if cmd.Flags().Changed("server") {
		if err := updateProfileServer(cmd, server); err != nil {
			pterm.Warning.Printfln("Could not update profile server: %v", err)
		}
	}

	pterm.Success.Printfln("Logged in to %s", pterm.Bold.Sprint(server))
	printTokenSummary(token)

	return nil
}

// updateProfileServer updates the active profile's server address if a profile is in use.
func updateProfileServer(cmd *cobra.Command, server string) error {
	cfg, err := cmdutil.LoadConfig()
	if err != nil {
		return err
	}

	profileName, _ := cmd.Flags().GetString("profile")

	name, profile := cmdutil.GetActiveProfile(cfg, profileName)
	if profile == nil || profile.Server == server {
		return nil
	}

	profile.Server = server
	cfg.Profiles[name] = *profile

	return cmdutil.SaveConfig(cfg)
}

// resolveLoginParams builds tokenParams from a bundle (file, stdin pipe) and/or flags.
// Flags explicitly passed on the command line override bundle values. Flags set
// only via environment variables (BindEnvToCommand) do NOT override the bundle;
// we use cmd.Flags().Changed() to distinguish explicitly-passed flags from
// env-var-derived ones.
func resolveLoginParams(cmd *cobra.Command) (tokenParams, error) {
	bundle, err := readBundle(cmd)
	if err != nil {
		return tokenParams{}, err
	}

	expiration, _ := cmd.Flags().GetDuration("expiration")

	var seed []byte

	// Start with flag values only if explicitly passed on the command line.
	var keyID, subject, signingKeyPath string
	var scopes []string

	if cmd.Flags().Changed("key-id") {
		keyID, _ = cmd.Flags().GetString("key-id")
	}

	if cmd.Flags().Changed("subject") {
		subject, _ = cmd.Flags().GetString("subject")
	}

	if cmd.Flags().Changed("scopes") {
		scopes, _ = cmd.Flags().GetStringSlice("scopes")
	}

	if cmd.Flags().Changed("signing-key") {
		signingKeyPath, _ = cmd.Flags().GetString("signing-key")
	}

	if bundle != nil {
		// Decode the hex seed from the bundle.
		decoded, err := hex.DecodeString(bundle.SigningKey)
		if err != nil {
			return tokenParams{}, fmt.Errorf("decoding bundle signingKey: %w", err)
		}

		seed = decoded

		// Bundle values fill in anything not explicitly set on the command line.
		if keyID == "" {
			keyID = bundle.KeyID
		}

		if subject == "" {
			subject = bundle.Subject
		}

		if len(scopes) == 0 {
			scopes = bundle.Scopes
		}
	}

	// If no bundle seed, fall back to --signing-key file.
	if seed == nil {
		if signingKeyPath == "" {
			return tokenParams{}, errors.New("either --bundle/stdin or --signing-key is required")
		}

		var loadErr error

		seed, loadErr = signing.LoadSeedFromFile(signingKeyPath)
		if loadErr != nil {
			return tokenParams{}, fmt.Errorf("loading signing key: %w", loadErr)
		}
	}

	if keyID == "" {
		return tokenParams{}, errors.New("required flag \"key-id\" not set")
	}

	if subject == "" {
		return tokenParams{}, errors.New("required flag \"subject\" not set")
	}

	return tokenParams{
		seed:       seed,
		keyID:      keyID,
		subject:    subject,
		scopes:     scopes,
		expiration: expiration,
	}, nil
}

// readBundle reads a key bundle from --bundle flag, explicit "-" for stdin, or
// a piped stdin (non-terminal). Returns nil if no bundle source is available.
func readBundle(cmd *cobra.Command) (*keyBundle, error) {
	bundlePath, _ := cmd.Flags().GetString("bundle")

	var data []byte

	switch {
	case bundlePath == "-":
		// Explicit stdin.
		var err error

		data, err = io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("reading bundle from stdin: %w", err)
		}
	case bundlePath != "":
		// File path.
		var err error

		data, err = os.ReadFile(bundlePath)
		if err != nil {
			return nil, fmt.Errorf("reading bundle file: %w", err)
		}
	case !term.IsTerminal(int(os.Stdin.Fd())):
		// Piped stdin (no --bundle flag but stdin is not a terminal).
		var err error

		data, err = io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("reading bundle from stdin: %w", err)
		}
		// If stdin was empty (e.g. redirected from /dev/null), treat as no bundle.
		if len(data) == 0 {
			return nil, nil
		}
	default:
		return nil, nil
	}

	var b keyBundle

	err := json.Unmarshal(data, &b)
	if err != nil {
		return nil, fmt.Errorf("parsing bundle JSON: %w", err)
	}

	return &b, nil
}

// printTokenSummary decodes the JWT (without verification) and displays a summary.
func printTokenSummary(tokenStr string) {
	parser := jwt.NewParser()
	claims := jwt.MapClaims{}

	_, _, err := parser.ParseUnverified(tokenStr, claims)
	if err != nil {
		pterm.Warning.Printfln("Could not decode token: %v", err)

		return
	}

	var rows [][]string
	if sub, _ := claims.GetSubject(); sub != "" {
		rows = append(rows, []string{"Subject", sub})
	}

	if iss, _ := claims.GetIssuer(); iss != "" {
		rows = append(rows, []string{"Issuer", iss})
	}

	if scopes, ok := claims["scope"].(string); ok && scopes != "" {
		rows = append(rows, []string{"Scopes", scopes})
	}

	if exp, _ := claims.GetExpirationTime(); exp != nil {
		remaining := time.Until(exp.Time)

		status := pterm.Green("valid")
		if remaining <= 0 {
			status = pterm.Red("EXPIRED")
		}

		rows = append(rows, []string{"Expires", fmt.Sprintf("%s (%s)", exp.Format(time.RFC3339), status)})
	}

	if len(rows) > 0 {
		data := append([][]string{{"Claim", "Value"}}, rows...)
		_ = pterm.DefaultTable.WithHasHeader().WithData(data).Render()
	}
}
