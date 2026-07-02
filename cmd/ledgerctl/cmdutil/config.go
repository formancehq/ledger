package cmdutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

// Profile holds connection settings for a named server environment.
type Profile struct {
	Server            string `json:"server"`
	Insecure          bool   `json:"insecure,omitempty"`
	TLSCaCert         string `json:"tlsCaCert,omitempty"`
	SigningKey        string `json:"signingKey,omitempty"`
	SigningKeyID      string `json:"signingKeyId,omitempty"`
	ResponseVerifyKey string `json:"responseVerifyKey,omitempty"`
}

// Config is the top-level configuration file for ledgerctl.
type Config struct {
	ActiveProfile string             `json:"activeProfile,omitempty"`
	Profiles      map[string]Profile `json:"profiles,omitempty"`
}

// ConfigDir returns the ledgerctl configuration directory
// (~/.config/ledgerctl on Linux, ~/Library/Application Support/ledgerctl on macOS).
func ConfigDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("determining config directory: %w", err)
	}

	return filepath.Join(base, "ledgerctl"), nil
}

// ConfigPath returns the path to the configuration file.
func ConfigPath() (string, error) {
	dir, err := ConfigDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "config.json"), nil
}

// LoadConfig reads the configuration file. If the file does not exist,
// a zero-value Config is returned (backward compatible with fresh installs).
func LoadConfig() (Config, error) {
	path, err := ConfigPath()
	if err != nil {
		return Config{}, err
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Config{}, nil
	}

	if err != nil {
		return Config{}, fmt.Errorf("reading config: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config: %w", err)
	}

	return cfg, nil
}

// SaveConfig writes the configuration file, creating the directory (0700)
// and file (0600) with restrictive permissions.
func SaveConfig(cfg Config) error {
	path, err := ConfigPath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("creating config directory: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	data = append(data, '\n')

	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	return nil
}

// ResolveProfileName returns the profile name the user selected via --profile
// or LEDGERCTL_PROFILE, and whether it was explicitly set (as opposed to
// falling back to cfg.ActiveProfile). Callers that need to distinguish
// "user asked for this profile" from "we fell back to the active one" must
// use the explicit flag: auth login uses it to bootstrap a new profile when
// the referenced --profile does not exist yet.
func ResolveProfileName(cmd *cobra.Command) (name string, explicit bool) {
	name, _ = cmd.Flags().GetString("profile")
	explicit = cmd.Flags().Changed("profile")

	if name == "" {
		if v := strings.TrimSpace(os.Getenv("LEDGERCTL_PROFILE")); v != "" {
			name = v
			explicit = true
		}
	}

	return name, explicit
}

// GetActiveProfile resolves which profile to use. If overrideName is non-empty,
// it is used; otherwise cfg.ActiveProfile is used. Returns nil if no profile
// is selected or the name is not found.
func GetActiveProfile(cfg Config, overrideName string) (string, *Profile) {
	name := overrideName
	if name == "" {
		name = cfg.ActiveProfile
	}

	if name == "" || cfg.Profiles == nil {
		return "", nil
	}

	p, ok := cfg.Profiles[name]
	if !ok {
		return name, nil
	}

	return name, &p
}

// ProfileFlagValue extracts the value for a CLI flag from a profile.
// Returns an empty string for flags the profile does not provide.
func ProfileFlagValue(p *Profile, flagName string) string {
	if p == nil {
		return ""
	}

	switch flagName {
	case "server":
		return p.Server
	case "insecure":
		if p.Insecure {
			return strconv.FormatBool(p.Insecure)
		}

		return ""
	case "tls-ca-cert":
		return p.TLSCaCert
	case "signing-key":
		return p.SigningKey
	case "signing-key-id":
		return p.SigningKeyID
	case "response-verify-key":
		return p.ResponseVerifyKey
	default:
		return ""
	}
}

// CompleteProfileNames is a cobra shell-completion function that suggests the
// connection profiles declared in the local config file. It is wired to the
// persistent --profile flag so pressing TAB lists the configured profile names.
//
// Completion runs in the user's interactive shell, so any failure (missing or
// malformed config) returns no suggestions rather than surfacing an error: a
// broken config must never disrupt tab completion.
func CompleteProfileNames(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	names := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		names = append(names, name)
	}

	sortStrings(names)

	return names, cobra.ShellCompDirectiveNoFileComp
}

// HasStoredToken checks whether the OS keychain contains a token for the given server address.
func HasStoredToken(kr Keyring, server string) bool {
	_, err := kr.Get(server)

	return err == nil
}
