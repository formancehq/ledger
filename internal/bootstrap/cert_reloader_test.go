package bootstrap

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/pkg/testserver"
)

func TestCertReloader_InitialLoad(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)
	r, err := NewCertReloader(TLSConfig{
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
		CAFile:   certs.CACertFile,
	})
	require.NoError(t, err)

	srv, err := r.GetCertificate(nil)
	require.NoError(t, err)
	require.NotEmpty(t, srv.Certificate)

	cli, err := r.GetClientCertificate(nil)
	require.NoError(t, err)
	require.NotEmpty(t, cli.Certificate)

	require.NotNil(t, r.ClientCAs())
}

func TestCertReloader_GetCertificate_ReturnsErrWhenUnset(t *testing.T) {
	t.Parallel()

	r, err := NewCertReloader(TLSConfig{})
	require.NoError(t, err)

	_, err = r.GetCertificate(nil)
	require.Error(t, err)
}

// withFastReloaderPolling shortens the poll fallback for the duration of a
// single test. Belt-and-braces against flaky fsnotify backends — and since
// fsnotify on macOS occasionally misses Rename events on symlink swaps, the
// polling fallback is the contract we care about anyway.
func withFastReloaderPolling(t *testing.T, d time.Duration) {
	t.Helper()
	prev := certReloaderPollInterval
	certReloaderPollInterval = d
	t.Cleanup(func() { certReloaderPollInterval = prev })
}

// TestCertReloader_ReloadsServerCertOnFileChange copies a fresh keypair over
// the cert/key paths and asserts that GetCertificate returns the new
// fingerprint without restarting the process. This is the regression contract
// for #348 — cert-manager-style rotations must be picked up live.
func TestCertReloader_ReloadsServerCertOnFileChange(t *testing.T) {
	withFastReloaderPolling(t, 50*time.Millisecond)

	// First keypair: written to the paths the reloader watches.
	dir := t.TempDir()
	first, err := testserver.GenerateTestCerts(dir)
	require.NoError(t, err)

	r, err := NewCertReloader(TLSConfig{
		CertFile: first.ServerCertFile,
		KeyFile:  first.ServerKeyFile,
		CAFile:   first.CACertFile,
	})
	require.NoError(t, err)

	logger := logging.Testing()
	ctx := t.Context()

	r.Start(ctx, logger)
	defer r.Stop()

	initial, err := r.GetCertificate(nil)
	require.NoError(t, err)
	initialFP := certFingerprint(t, initial.Certificate[0])

	// Second keypair: generated in a sibling temp dir, then byte-copied over
	// the paths the reloader is watching. fsnotify should pick this up;
	// the polling fallback would catch it within a minute anyway.
	swapDir := t.TempDir()
	second, err := testserver.GenerateTestCerts(swapDir)
	require.NoError(t, err)

	copyFile(t, second.ServerCertFile, first.ServerCertFile)
	copyFile(t, second.ServerKeyFile, first.ServerKeyFile)

	require.Eventually(t, func() bool {
		cert, getErr := r.GetCertificate(nil)
		if getErr != nil {
			return false
		}

		return certFingerprint(t, cert.Certificate[0]) != initialFP
	}, 10*time.Second, 50*time.Millisecond,
		"GetCertificate must return the rotated cert after the file is replaced")
}

// TestCertReloader_ReloadsCAOnFileChange covers the trust-pool side: a fresh
// CA bundle on disk must rebuild the *x509.CertPool the server uses to verify
// client certs.
func TestCertReloader_ReloadsCAOnFileChange(t *testing.T) {
	withFastReloaderPolling(t, 50*time.Millisecond)

	dir := t.TempDir()
	first, err := testserver.GenerateTestCerts(dir)
	require.NoError(t, err)

	r, err := NewCertReloader(TLSConfig{
		CertFile: first.ServerCertFile,
		KeyFile:  first.ServerKeyFile,
		CAFile:   first.CACertFile,
	})
	require.NoError(t, err)

	logger := logging.Testing()
	ctx := t.Context()

	r.Start(ctx, logger)
	defer r.Stop()

	initialPool := r.ClientCAs()
	require.NotNil(t, initialPool)

	// Replace the CA file with a completely fresh one from a different
	// testserver.GenerateTestCerts run.
	swapDir := t.TempDir()
	second, err := testserver.GenerateTestCerts(swapDir)
	require.NoError(t, err)
	copyFile(t, second.CACertFile, first.CACertFile)

	require.Eventually(t, func() bool {
		pool := r.ClientCAs()
		// Pointer inequality is the contract: reload swaps the *CertPool
		// only when the PEM bytes actually changed, so a new pointer means
		// a new CA bundle was applied.
		return pool != nil && pool != initialPool
	}, 10*time.Second, 50*time.Millisecond,
		"ClientCAs must rebuild after the CA file is replaced")
}

// TestCertReloader_StopIsIdempotent guards against double-close panics from
// callers that don't centralize lifecycle (CLI tests, parallel fx graphs).
func TestCertReloader_StopIsIdempotent(t *testing.T) {
	t.Parallel()

	r, err := NewCertReloader(TLSConfig{})
	require.NoError(t, err)

	r.Stop()
	r.Stop() // must not panic
}

// TestCertReloader_StartWithoutStartIsNoOp ensures short-lived callers that
// never wire Stop don't leak goroutines: with Start uninvoked, no goroutine
// exists in the first place, and GetCertificate keeps working off the initial
// load.
func TestCertReloader_StartWithoutStartIsNoOp(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)
	r, err := NewCertReloader(TLSConfig{
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
	})
	require.NoError(t, err)

	got, err := r.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Stop() without Start() must still be safe.
	r.Stop()
}

// certFingerprint returns a sha256 of the DER-encoded leaf, so two distinct
// keypairs always produce distinct fingerprints even when the test fixture
// reuses the same serial/subject (testserver.GenerateTestCerts hardcodes both).
func certFingerprint(t *testing.T, der []byte) string {
	t.Helper()
	// Parse to make sure it's a valid cert; the hash itself is taken from
	// the raw DER so any change to the underlying public key is observable.
	_, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	sum := sha256.Sum256(der)

	return hex.EncodeToString(sum[:])
}

// copyFile copies src over dst atomically enough for our reload tests: we
// write a tempfile next to dst then rename. The rename should fire a
// Create/Rename event on most fsnotify backends.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)

	tmp, err := os.CreateTemp(filepath.Dir(dst), "swap-*")
	require.NoError(t, err)

	_, err = tmp.Write(data)
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	require.NoError(t, os.Rename(tmp.Name(), dst))
}
