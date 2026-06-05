package cmdutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// newTLSFlagCommand returns a cobra command with the same TLS-related
// persistent flags ledgerctl registers in main, but minimal enough to drive
// GetClientTransportCredentials from a unit test.
func newTLSFlagCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("insecure", false, "")
	cmd.Flags().String("tls-ca-cert", "", "")

	return cmd
}

// Regression: a stray INSECURE=true in the environment (notoriously set by
// `ENV INSECURE=true` in the project's Dockerfiles before this fix) used to
// silently win over an explicit --tls-ca-cert, sending ledgerctl in plaintext
// to a tls-mode=required server with the symptom "error reading server preface:
// EOF". GetClientTransportCredentials must now refuse the conflict instead of
// quietly picking insecure.
func TestGetClientTransportCredentials_InsecureWithCACertIsRejected(t *testing.T) {
	t.Parallel()

	cmd := newTLSFlagCommand()
	require.NoError(t, cmd.Flags().Set("insecure", "true"))
	require.NoError(t, cmd.Flags().Set("tls-ca-cert", "/tls/ca.crt"))

	_, err := GetClientTransportCredentials(cmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--insecure and --tls-ca-cert are mutually exclusive")
}

func TestGetClientTransportCredentials_InsecureOnly(t *testing.T) {
	t.Parallel()

	cmd := newTLSFlagCommand()
	require.NoError(t, cmd.Flags().Set("insecure", "true"))

	creds, err := GetClientTransportCredentials(cmd)
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestGetClientTransportCredentials_TLSWithCustomCA(t *testing.T) {
	t.Parallel()

	caPath := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(caPath, writeTestCA(t), 0o600))

	cmd := newTLSFlagCommand()
	require.NoError(t, cmd.Flags().Set("tls-ca-cert", caPath))

	creds, err := GetClientTransportCredentials(cmd)
	require.NoError(t, err)
	require.NotNil(t, creds)
}

// writeTestCA generates a self-signed CA cert and returns the PEM bytes.
// The cert is only used to exercise the AppendCertsFromPEM path — no
// connection is opened in the unit test.
func writeTestCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Unit Test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
