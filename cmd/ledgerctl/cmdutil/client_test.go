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
	cmd.Flags().String("tls-server-name", "", "")

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

// --tls-server-name only makes sense when TLS is active; pairing it with
// --insecure is rejected for the same reason as --tls-ca-cert — it catches a
// stray LEDGERCTL_INSECURE leaking in and silently disabling verification.
func TestGetClientTransportCredentials_InsecureWithServerNameIsRejected(t *testing.T) {
	t.Parallel()

	cmd := newTLSFlagCommand()
	require.NoError(t, cmd.Flags().Set("insecure", "true"))
	require.NoError(t, cmd.Flags().Set("tls-server-name", "ledger.svc.cluster.local"))

	_, err := GetClientTransportCredentials(cmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--insecure and --tls-server-name are mutually exclusive")
}

// A --tls-server-name without --insecure is the supported case: dial by IP,
// verify against the cert SANs by name. This is what unblocks ledgerctl from
// inside a TLS cluster pod, where the operator-issued cert covers the in-cluster
// DNS names but never 127.0.0.1/localhost.
func TestGetClientTransportCredentials_TLSWithServerName(t *testing.T) {
	t.Parallel()

	cmd := newTLSFlagCommand()
	require.NoError(t, cmd.Flags().Set("tls-server-name", "ledger.svc.cluster.local"))

	creds, err := GetClientTransportCredentials(cmd)
	require.NoError(t, err)
	require.NotNil(t, creds)

	// Assert the override actually reached the tls.Config — plain TLS creds are
	// also non-nil, so only checking creds is not enough to guard the
	// load-bearing ServerName assignment.
	cfg, err := buildClientTLSConfig("", "ledger.svc.cluster.local")
	require.NoError(t, err)
	require.Equal(t, "ledger.svc.cluster.local", cfg.ServerName)
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

// ValidateTLSFlags is the single source of truth for the mutual-exclusion
// rules, shared by the connection path and the profile-persistence paths
// (profile create, auth login). Cover it directly so a persistence caller that
// forgets to route through it can't quietly regress the guard.
func TestValidateTLSFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		insecure   bool
		caCert     string
		serverName string
		wantErr    string
	}{
		{name: "no flags is valid"},
		{name: "tls-ca-cert alone is valid", caCert: "/tls/ca.crt"},
		{name: "tls-server-name alone is valid", serverName: "ledger.svc.cluster.local"},
		{name: "insecure alone is valid", insecure: true},
		{
			name:     "insecure + tls-ca-cert rejected",
			insecure: true, caCert: "/tls/ca.crt",
			wantErr: "--insecure and --tls-ca-cert are mutually exclusive",
		},
		{
			name:     "insecure + tls-server-name rejected",
			insecure: true, serverName: "ledger.svc.cluster.local",
			wantErr: "--insecure and --tls-server-name are mutually exclusive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateTLSFlags(tc.insecure, tc.caCert, tc.serverName)
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
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
