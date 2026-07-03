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

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
)

// keyBundle is the JSON format for agent key bundles produced by
// `kubectl ledger agents get-key --bundle`.
type keyBundle struct {
	SigningKey string   `json:"signingKey"`
	KeyID      string   `json:"keyId"`
	Scopes     []string `json:"scopes"`
	Subject    string   `json:"subject"`
	God        bool     `json:"god"`
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
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runLogin,
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

	// Order: keychain first, then profile. If profile sync fails, roll back
	// the just-stored token so we never leave the config pointing at a
	// server whose keychain entry is absent (or worse, points at the wrong
	// token). Bootstrap is an advertised feature of `auth login --profile
	// <new>`, and this two-phase commit is the only way to keep exit code,
	// keychain, and config aligned without introducing a lock file.
	keyring := cmdutil.GetKeyring(cmd)
	if err := keyring.Set(server, token); err != nil {
		return fmt.Errorf("storing token in keychain: %w", err)
	}

	if err := syncProfile(cmd, server); err != nil {
		if delErr := keyring.Delete(server); delErr != nil {
			return fmt.Errorf("syncing profile: %w (also failed to remove stranded keychain token: %v)", err, delErr)
		}

		return fmt.Errorf("syncing profile: %w", err)
	}

	pterm.Success.Printfln("Logged in to %s", pterm.Bold.Sprint(server))
	printTokenSummary(token)

	return nil
}

// syncProfile keeps the referenced profile aligned with the login just
// completed:
//
//   - If --profile <name> (or LEDGERCTL_PROFILE) points at a profile that
//     does not exist yet, bootstrap it from the current connection flags so
//     subsequent commands with --profile <name> find the token keyed by the
//     same server address.
//   - If the profile already exists and --server was explicitly passed on
//     the CLI, update the profile's server address so subsequent commands
//     look up the keychain under the address we just stored the token under.
//   - Otherwise do nothing.
//
// Changed("server") is trusted to mean "user typed --server on the CLI":
// resolveFlag in main.go applies env/profile values through Flag.Value.Set,
// which does not touch the Changed bit.
func syncProfile(cmd *cobra.Command, server string) error {
	profileName, profileExplicit := cmdutil.ResolveProfileName(cmd)

	cfg, err := cmdutil.LoadConfig()
	if err != nil {
		return err
	}

	// Fall back to the active profile so `auth login --server new` (no
	// --profile) still syncs the profile the rest of the session uses.
	// Without this fallback the token gets stored under `new`, but subsequent
	// commands resolve the active profile's old server and can't find it.
	if profileName == "" {
		profileName = cfg.ActiveProfile
	}

	if profileName == "" {
		return nil
	}

	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]cmdutil.Profile)
	}

	if existing, ok := cfg.Profiles[profileName]; ok {
		if !cmd.Flags().Changed("server") || existing.Server == server {
			return nil
		}

		existing.Server = server
		cfg.Profiles[profileName] = existing

		return cmdutil.SaveConfig(cfg)
	}

	if !profileExplicit {
		// The name came from cfg.ActiveProfile but the entry was deleted from
		// under us; do not silently recreate it.
		return nil
	}

	insecure, _ := cmd.Flags().GetBool("insecure")
	tlsCaCert, _ := cmd.Flags().GetString("tls-ca-cert")
	signingKey, _ := cmd.Flags().GetString("signing-key")
	responseVerifyKey, _ := cmd.Flags().GetString("response-verify-key")

	// signingKeyId identifies the same key entry as auth login's local
	// --key-id, but the two flags rarely both get set: users typically pass
	// one of them. Prefer --signing-key-id when present, fall back to
	// --key-id so a `--profile prod --key-id prod-key` bootstrap persists a
	// signingKeyId that the profile-derived --key-id fallback can read on
	// subsequent logins.
	signingKeyID, _ := cmd.Flags().GetString("signing-key-id")
	if signingKeyID == "" {
		signingKeyID, _ = cmd.Flags().GetString("key-id")
	}

	cfg.Profiles[profileName] = cmdutil.Profile{
		Server:            server,
		Insecure:          insecure,
		TLSCaCert:         tlsCaCert,
		SigningKey:        signingKey,
		SigningKeyID:      signingKeyID,
		ResponseVerifyKey: responseVerifyKey,
	}

	// Activate the new profile if this is the very first one OR the config
	// currently has no active profile: without the second branch, running
	// `auth login --profile new` on a config with an orphaned/blank
	// activeProfile would leave commands without --profile falling back to
	// the localhost default and missing the token we just stored.
	if len(cfg.Profiles) == 1 || cfg.ActiveProfile == "" {
		cfg.ActiveProfile = profileName
	}

	return cmdutil.SaveConfig(cfg)
}

// resolveKeyID returns the effective JWT key ID for an `auth` command,
// consolidating the sibling --signing-key-id fallback so `auth login` and
// `auth generate-token` share the same behavior.
//
// The two flags identify the same key entry, and users routinely pass only
// one of them. Whenever --key-id was NOT explicitly typed on the CLI, this
// helper prefers --signing-key-id's value regardless of its source (CLI,
// LEDGERCTL_SIGNING_KEY_ID, or the profile). This covers:
//   - CLI --signing-key-id overriding a profile-derived --key-id:
//     `auth login --signing-key-id new` against an active profile with
//     signingKeyId=old must sign the JWT with `new`, not `old`.
//   - env LEDGERCTL_SIGNING_KEY_ID doing the same (env leaves Changed=false
//     via Flag.Value.Set in cmdutil's resolveFlag).
//   - Bootstrap with only --signing-key-id and no profile fallback.
//
// Bundle values still win against an env/profile-derived --signing-key-id via
// the downstream bundle-override guard in resolveLoginParams; a CLI --key-id
// still wins against everything via the Changed check here.
func resolveKeyID(cmd *cobra.Command) string {
	keyID, _ := cmd.Flags().GetString("key-id")
	if cmd.Flags().Changed("key-id") {
		return keyID
	}

	if sk, _ := cmd.Flags().GetString("signing-key-id"); sk != "" {
		return sk
	}

	return keyID
}

// resolveLoginParams builds tokenParams from a bundle (file, stdin pipe) and/or
// flags. Precedence for each field, highest to lowest: CLI flag > bundle field
// > env/profile-derived flag value > zero value. cmd.Flags().Changed() is
// trusted to mean "the user typed the flag on the CLI" — env-derived values
// coming through cmdutil's owned-flag resolveFlag path leave Changed=false, so
// they don't spuriously beat the bundle.
func resolveLoginParams(cmd *cobra.Command) (tokenParams, error) {
	bundle, err := readBundle(cmd)
	if err != nil {
		return tokenParams{}, err
	}

	expiration, _ := cmd.Flags().GetDuration("expiration")

	// Read flag values regardless of Changed: they may have been filled by
	// PersistentPreRunE from the active profile (e.g. profile.signingKey ->
	// --signing-key, profile.signingKeyId -> --key-id).
	keyID := resolveKeyID(cmd)
	subject, _ := cmd.Flags().GetString("subject")
	scopes, _ := cmd.Flags().GetStringSlice("scopes")
	signingKeyPath, _ := cmd.Flags().GetString("signing-key")

	var seed []byte

	if bundle != nil {
		decoded, err := hex.DecodeString(bundle.SigningKey)
		if err != nil {
			return tokenParams{}, fmt.Errorf("decoding bundle signingKey: %w", err)
		}

		seed = decoded

		// The bundle wins over env/profile-derived flag values, but explicit
		// CLI flags (Changed=true) override the bundle. --signing-key-id is
		// the sibling flag whose value we fall back to for keyID above, so
		// an explicit --signing-key-id must protect keyID from the bundle
		// too.
		if !cmd.Flags().Changed("key-id") && !cmd.Flags().Changed("signing-key-id") && bundle.KeyID != "" {
			keyID = bundle.KeyID
		}

		if !cmd.Flags().Changed("subject") && bundle.Subject != "" {
			subject = bundle.Subject
		}

		if !cmd.Flags().Changed("scopes") && len(bundle.Scopes) > 0 {
			scopes = bundle.Scopes
		}
	}

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

	god, _ := cmd.Flags().GetBool("god")
	if !cmd.Flags().Changed("god") && bundle != nil {
		god = bundle.God
	}

	return tokenParams{
		seed:       seed,
		keyID:      keyID,
		subject:    subject,
		scopes:     scopes,
		expiration: expiration,
		god:        god,
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

	if god, ok := claims["god"].(bool); ok && god {
		rows = append(rows, []string{"God mode", pterm.Yellow("enabled")})
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
