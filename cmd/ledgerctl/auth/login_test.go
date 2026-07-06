package auth

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// newTestCmd assembles a bare login-like cobra command carrying the
// persistent flags PersistentPreRunE would normally hand down from root
// (--profile, --server, --tls-ca-cert, --insecure, --signing-key,
// --signing-key-id, --response-verify-key) plus the local --key-id auth
// login declares. syncProfile reads them directly off cmd.Flags(), so we
// skip the full root wiring and drive it in isolation.
func newTestCmd(t *testing.T) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: "login"}
	cmd.Flags().String("profile", "", "")
	cmd.Flags().String("server", "localhost:8888", "")
	cmd.Flags().Bool("insecure", false, "")
	cmd.Flags().String("tls-ca-cert", "", "")
	cmd.Flags().String("signing-key", "", "")
	cmd.Flags().String("signing-key-id", "", "")
	cmd.Flags().String("response-verify-key", "", "")
	cmd.Flags().String("key-id", "", "")
	cmd.SetContext(context.Background())

	return cmd
}

// pinConfig points cmdutil.LoadConfig() / SaveConfig() at a hermetic temp
// directory so syncProfile round-trips through a real config.json without
// touching the user's environment.
func pinConfig(t *testing.T) {
	t.Helper()

	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("APPDATA", tmp)
	t.Setenv("LOCALAPPDATA", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")
}

// fakeKeyring is a hermetic Keyring for testing runLogin's rollback path.
// It records every call so tests can assert that a syncProfile failure
// either restores the prior credential (re-login case) or removes the
// just-stored token (bootstrap / no prior token case).
type fakeKeyring struct {
	store   map[string]string
	setCall int
	delCall int
	setFail error
	delFail error
	getFail error // when set, Get returns this error (bypasses store lookup)
}

func (f *fakeKeyring) Get(server string) (string, error) {
	if f.getFail != nil {
		return "", f.getFail
	}

	if f.store == nil {
		return "", cmdutil.ErrTokenNotFound
	}

	t, ok := f.store[server]
	if !ok {
		return "", cmdutil.ErrTokenNotFound
	}

	return t, nil
}

func (f *fakeKeyring) Set(server, token string) error {
	f.setCall++

	if f.setFail != nil {
		return f.setFail
	}

	if f.store == nil {
		f.store = make(map[string]string)
	}

	f.store[server] = token

	return nil
}

func (f *fakeKeyring) Delete(server string) error {
	f.delCall++

	if f.delFail != nil {
		return f.delFail
	}

	delete(f.store, server)

	return nil
}

// TestRunLogin_RollbackRestoresPriorToken guards the two-phase-commit
// rollback: when syncProfile fails after keyring.Set overwrote a
// pre-existing credential, runLogin must restore that credential rather
// than delete it unconditionally. Otherwise a re-login whose config-write
// step fails would wipe the user's still-valid previous session.
func TestRunLogin_RollbackRestoresPriorToken(t *testing.T) {
	// Point HOME at a read-only path AFTER the initial pre-existing token is
	// seeded, so syncProfile's SaveConfig fails and triggers the rollback.
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("APPDATA", tmp)
	t.Setenv("LOCALAPPDATA", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")

	// Seed a valid config first so LoadConfig succeeds, then make the config
	// directory read-only so SaveConfig inside syncProfile fails.
	require.NoError(t, cmdutil.SaveConfig(cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "prod.example.com:8888"},
		},
	}))

	// Make the config file itself read-only so SaveConfig's WriteFile fails
	// while LoadConfig still succeeds. Chmod'ing the parent dir wouldn't
	// work: overwriting an existing writable inode doesn't require dir
	// write perms on macOS/Linux.
	configPath, err := cmdutil.ConfigPath()
	require.NoError(t, err)
	require.NoError(t, os.Chmod(configPath, 0o400))
	t.Cleanup(func() { _ = os.Chmod(configPath, 0o600) })

	// Fake keyring seeded with the prior session token.
	kr := &fakeKeyring{store: map[string]string{"prod.example.com:9999": "prior-token"}}

	seedPath := filepath.Join(t.TempDir(), "seed.hex")
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(seedPath, []byte(hex.EncodeToString(seed)), 0o600))

	// Reproduce the flag surface PersistentPreRunE would hand down: root's
	// --profile, --server, --tls-ca-cert, --insecure, --signing-key-id,
	// --response-verify-key, plus login's own local flags.
	cmd := NewLoginCommand()
	cmd.Flags().String("profile", "", "")
	cmd.Flags().String("server", "localhost:8888", "")
	cmd.Flags().Bool("insecure", false, "")
	cmd.Flags().String("tls-ca-cert", "", "")
	cmd.Flags().String("signing-key-id", "", "")
	cmd.Flags().String("response-verify-key", "", "")
	cmd.SetContext(cmdutil.WithKeyring(context.Background(), kr))
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:9999",
		"--signing-key", seedPath,
		"--key-id", "k",
		"--subject", "svc",
	}))

	// runLogin must return an error (syncProfile fails) and the keyring
	// must be back to "prior-token", not empty.
	err = runLogin(cmd, nil)
	require.Error(t, err, "runLogin must surface the syncProfile failure")

	stored, getErr := kr.Get("prod.example.com:9999")
	require.NoError(t, getErr,
		"prior credential must be restored, not deleted")
	require.Equal(t, "prior-token", stored)

	require.Equal(t, 0, kr.delCall,
		"rollback must NOT call Delete when a prior credential existed")
}

// TestRunLogin_SkipsRollbackOnOpaqueKeyringGetError guards against wiping a
// prior credential we could not read: when keyring.Get fails with an error
// that is NOT ErrTokenNotFound (transient backend, item permission issue),
// runLogin's rollback must NOT call Delete — the store could still hold a
// valid prior token we're unable to see. Report the sync failure and the
// unknown-state hint; leave the just-stored token in place.
func TestRunLogin_SkipsRollbackOnOpaqueKeyringGetError(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("APPDATA", tmp)
	t.Setenv("LOCALAPPDATA", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")

	require.NoError(t, cmdutil.SaveConfig(cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "prod.example.com:8888"},
		},
	}))

	configPath, err := cmdutil.ConfigPath()
	require.NoError(t, err)
	require.NoError(t, os.Chmod(configPath, 0o400))
	t.Cleanup(func() { _ = os.Chmod(configPath, 0o600) })

	// Fake keyring: Get errors with a NON-ErrTokenNotFound error, Set works.
	kr := &fakeKeyring{getFail: errors.New("keychain backend transient error")}

	seedPath := filepath.Join(t.TempDir(), "seed.hex")
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(seedPath, []byte(hex.EncodeToString(seed)), 0o600))

	cmd := NewLoginCommand()
	cmd.Flags().String("profile", "", "")
	cmd.Flags().String("server", "localhost:8888", "")
	cmd.Flags().Bool("insecure", false, "")
	cmd.Flags().String("tls-ca-cert", "", "")
	cmd.Flags().String("signing-key-id", "", "")
	cmd.Flags().String("response-verify-key", "", "")
	cmd.SetContext(cmdutil.WithKeyring(context.Background(), kr))
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:9999",
		"--signing-key", seedPath,
		"--key-id", "k",
		"--subject", "svc",
	}))

	err = runLogin(cmd, nil)
	require.Error(t, err)

	require.Equal(t, 0, kr.delCall,
		"Delete must NOT run when the prior keychain state is unknown")
	require.Equal(t, 1, kr.setCall,
		"Set runs exactly once (initial store); no restore-Set on opaque prev-Get error")
	require.Contains(t, err.Error(), "keychain rollback skipped",
		"error must surface that rollback was intentionally skipped")
}

// TestResolveLoginParams_CLISigningKeyIDBeatsProfileValue guards the
// documented CLI-over-profile precedence for --signing-key-id: when
// PersistentPreRunE's resolveFlag has populated --signing-key-id with the
// profile's signingKeyId (Value.Set, Changed=false), a subsequent CLI
// --signing-key-id must beat it.
func TestResolveLoginParams_CLISigningKeyIDBeatsProfileValue(t *testing.T) {
	seedPath := filepath.Join(t.TempDir(), "seed.hex")
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(seedPath, []byte(hex.EncodeToString(seed)), 0o600))

	cmd := newTestCmd(t)
	cmd.Flags().StringSlice("scopes", nil, "")
	cmd.Flags().Duration("expiration", 0, "")
	cmd.Flags().Bool("god", false, "")
	cmd.Flags().String("subject", "", "")
	cmd.Flags().String("bundle", "", "")

	// Simulate PersistentPreRunE's resolveFlag having applied the profile
	// value via Value.Set (Changed=false).
	require.NoError(t, cmd.Flags().Lookup("signing-key-id").Value.Set("profile-old"))
	require.False(t, cmd.Flags().Changed("signing-key-id"),
		"guard: profile-derived --signing-key-id must leave Changed=false")

	// User overrides via CLI.
	require.NoError(t, cmd.ParseFlags([]string{
		"--signing-key", seedPath,
		"--signing-key-id", "cli-new",
		"--subject", "svc",
	}))

	p, err := resolveLoginParams(cmd)
	require.NoError(t, err)
	require.Equal(t, "cli-new", p.keyID,
		"CLI --signing-key-id must beat a profile-derived value")
}

// TestResolveLoginParams_BundleBeatsProfileDerivedKeyID guards the documented
// precedence CLI > bundle > profile: a keyID resolved from the profile
// (Changed=false) must be superseded by a bundle keyId when no CLI --key-id
// / --signing-key-id was passed.
func TestResolveLoginParams_BundleBeatsProfileDerivedKeyID(t *testing.T) {
	dir := t.TempDir()

	seedBytes := make([]byte, 32)
	for i := range seedBytes {
		seedBytes[i] = byte(i + 1)
	}
	bundle := keyBundle{
		SigningKey: hex.EncodeToString(seedBytes),
		KeyID:      "bundle-key-id",
		Subject:    "bundle-subject",
	}
	payload, err := json.Marshal(bundle)
	require.NoError(t, err)

	bundlePath := filepath.Join(dir, "bundle.json")
	require.NoError(t, os.WriteFile(bundlePath, payload, 0o600))

	cmd := newTestCmd(t)
	cmd.Flags().StringSlice("scopes", nil, "")
	cmd.Flags().Duration("expiration", 0, "")
	cmd.Flags().Bool("god", false, "")
	cmd.Flags().String("subject", "", "")
	cmd.Flags().String("bundle", "", "")

	// Simulate PersistentPreRunE having resolved --key-id from the active
	// profile: value populated via Value.Set (Changed=false).
	require.NoError(t, cmd.Flags().Lookup("key-id").Value.Set("profile-derived"))
	require.False(t, cmd.Flags().Changed("key-id"),
		"guard: profile-derived key-id must leave Changed=false")

	require.NoError(t, cmd.ParseFlags([]string{"--bundle", bundlePath}))

	p, err := resolveLoginParams(cmd)
	require.NoError(t, err)
	require.Equal(t, "bundle-key-id", p.keyID,
		"bundle keyId must beat a profile-derived --key-id when no CLI flag is passed")
}

// TestResolveLoginParams_FallsBackToSigningKeyIDForKeyID covers the bootstrap
// path where the user passes --signing-key-id (matching the profile-config
// field name) but not --key-id. On a first login there is no profile for
// PersistentPreRunE's profile.signingKeyId -> --key-id fallback to read
// from, so resolveLoginParams must accept --signing-key-id as the JWT key
// ID; otherwise the command fails with `required flag "key-id" not set`
// even though the user provided the equivalent information.
func TestResolveLoginParams_FallsBackToSigningKeyIDForKeyID(t *testing.T) {
	// Write a valid 32-byte seed so signing.LoadSeedFromFile succeeds.
	seedPath := filepath.Join(t.TempDir(), "seed.hex")
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(seedPath, []byte(hex.EncodeToString(seed)), 0o600))

	cmd := newTestCmd(t)
	cmd.Flags().StringSlice("scopes", nil, "")
	cmd.Flags().Duration("expiration", 0, "")
	cmd.Flags().Bool("god", false, "")
	cmd.Flags().String("subject", "", "")
	cmd.Flags().String("bundle", "", "")

	require.NoError(t, cmd.ParseFlags([]string{
		"--signing-key", seedPath,
		"--signing-key-id", "prod-key",
		"--subject", "svc",
	}))

	// Stdin is a terminal in `go test`, so readBundle returns (nil, nil).
	p, err := resolveLoginParams(cmd)
	require.NoError(t, err, "--signing-key-id alone must satisfy --key-id at bootstrap")
	require.Equal(t, "prod-key", p.keyID)
}

// TestResolveLoginParams_ExplicitSigningKeyIDBeatsBundle guards the "CLI
// overrides bundle" precedence when the CLI-passed flag is --signing-key-id
// (the sibling that also feeds keyID via the bootstrap fallback). Without
// this, a user calling `auth login --bundle b.json --signing-key-id foo`
// would silently get bundle.KeyID in the JWT because the bundle-override
// guard only checked Changed("key-id").
func TestResolveLoginParams_ExplicitSigningKeyIDBeatsBundle(t *testing.T) {
	dir := t.TempDir()

	seedBytes := make([]byte, 32)
	for i := range seedBytes {
		seedBytes[i] = byte(i + 1)
	}
	seedHex := hex.EncodeToString(seedBytes)

	bundle := keyBundle{
		SigningKey: seedHex,
		KeyID:      "bundle-key-id",
		Subject:    "bundle-subject",
	}
	bundlePath := filepath.Join(dir, "bundle.json")
	payload, err := json.Marshal(bundle)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(bundlePath, payload, 0o600))

	cmd := newTestCmd(t)
	cmd.Flags().StringSlice("scopes", nil, "")
	cmd.Flags().Duration("expiration", 0, "")
	cmd.Flags().Bool("god", false, "")
	cmd.Flags().String("subject", "", "")
	cmd.Flags().String("bundle", "", "")

	require.NoError(t, cmd.ParseFlags([]string{
		"--bundle", bundlePath,
		"--signing-key-id", "cli-key-id",
	}))

	p, err := resolveLoginParams(cmd)
	require.NoError(t, err)
	require.Equal(t, "cli-key-id", p.keyID,
		"explicit --signing-key-id must beat bundle.KeyID (CLI over bundle)")
}

func TestSyncProfile_BootstrapsMissingProfile(t *testing.T) {
	pinConfig(t)

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:8888",
		"--signing-key", "/keys/prod.hex",
		"--signing-key-id", "prod-key",
		"--tls-ca-cert", "/tls/ca.pem",
	}))

	require.NoError(t, syncProfile(cmd, "prod.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)

	profile, ok := cfg.Profiles["prod"]
	require.True(t, ok, "auth login --profile <new> must persist the profile")
	require.Equal(t, "prod.example.com:8888", profile.Server)
	require.Equal(t, "/keys/prod.hex", profile.SigningKey)
	require.Equal(t, "prod-key", profile.SigningKeyID)
	require.Equal(t, "/tls/ca.pem", profile.TLSCaCert)
	require.Equal(t, "prod", cfg.ActiveProfile,
		"first profile must be activated")
}

func TestSyncProfile_UpdatesServerOnExplicitFlag(t *testing.T) {
	pinConfig(t)

	// Seed an existing profile.
	seed := cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "old.example.com:8888"},
		},
	}
	require.NoError(t, cmdutil.SaveConfig(seed))

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "new.example.com:8888",
	}))

	require.NoError(t, syncProfile(cmd, "new.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "new.example.com:8888", cfg.Profiles["prod"].Server,
		"explicit --server must rewrite the profile so the keychain lookup finds the just-stored token")
}

func TestSyncProfile_DoesNotRewriteOnEnvOnlyServer(t *testing.T) {
	pinConfig(t)

	seed := cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "prod.example.com:8888"},
		},
	}
	require.NoError(t, cmdutil.SaveConfig(seed))

	// Simulate PersistentPreRunE having resolved --server from
	// LEDGERCTL_SERVER: value present, Changed=false (that's what
	// resolveFlag does now).
	cmd := newTestCmd(t)
	require.NoError(t, cmd.Flags().Lookup("server").Value.Set("env.example.com:1234"))
	require.NoError(t, cmd.Flags().Lookup("profile").Value.Set("prod"))

	require.False(t, cmd.Flags().Changed("server"),
		"guard: env-derived server must leave Changed=false")

	require.NoError(t, syncProfile(cmd, "env.example.com:1234"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "prod.example.com:8888", cfg.Profiles["prod"].Server,
		"env-only server divergence must not rewrite the profile")
}

func TestSyncProfile_NoProfileFlagIsNoOp(t *testing.T) {
	pinConfig(t)

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--server", "some.example.com:8888",
	}))

	require.NoError(t, syncProfile(cmd, "some.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Empty(t, cfg.Profiles,
		"auth login without --profile must not touch profiles")
}

func TestSyncProfile_UpdatesActiveProfileWhenFlagOmitted(t *testing.T) {
	pinConfig(t)

	// Existing active profile.
	seed := cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "old.example.com:8888"},
		},
	}
	require.NoError(t, cmdutil.SaveConfig(seed))

	// User re-logs into a new address without --profile: syncProfile must
	// still update the active profile, otherwise the just-stored token sits
	// under new.example.com:9999 but every command with the active profile
	// resolves old.example.com:8888 and cannot find the token.
	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--server", "new.example.com:9999",
	}))

	require.NoError(t, syncProfile(cmd, "new.example.com:9999"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "new.example.com:9999", cfg.Profiles["prod"].Server,
		"active profile must track the server the token was just stored under")
}

func TestSyncProfile_BootstrapUsesKeyIDForSigningKeyID(t *testing.T) {
	pinConfig(t)

	// User bootstraps with --key-id (the local auth login flag) but does
	// not set the persistent --signing-key-id: the profile must still
	// persist a signingKeyId so a later `auth login --profile prod` can
	// use the profile-derived --key-id fallback.
	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:8888",
		"--signing-key", "/keys/prod.hex",
		"--key-id", "prod-key",
	}))

	require.NoError(t, syncProfile(cmd, "prod.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "prod-key", cfg.Profiles["prod"].SigningKeyID,
		"--key-id must be persisted as signingKeyId when --signing-key-id is not set")
}

func TestSyncProfile_BootstrapPrefersSigningKeyIDOverKeyID(t *testing.T) {
	pinConfig(t)

	// If both flags are set they should typically match; if they don't, the
	// explicit --signing-key-id takes precedence — that's the flag whose
	// name matches the persisted field.
	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:8888",
		"--signing-key", "/keys/prod.hex",
		"--signing-key-id", "explicit-signing-id",
		"--key-id", "jwt-only-id",
	}))

	require.NoError(t, syncProfile(cmd, "prod.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "explicit-signing-id", cfg.Profiles["prod"].SigningKeyID)
}

func TestSyncProfile_ActiveButDeletedIsNotResurrected(t *testing.T) {
	pinConfig(t)

	// A stale activeProfile pointer with no matching entry: this is a broken
	// state we should not silently repair on the next login.
	seed := cmdutil.Config{ActiveProfile: "ghost"}
	require.NoError(t, cmdutil.SaveConfig(seed))

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--server", "some.example.com:8888",
	}))

	require.NoError(t, syncProfile(cmd, "some.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Empty(t, cfg.Profiles,
		"a dangling activeProfile must not be resurrected by auth login")
}
