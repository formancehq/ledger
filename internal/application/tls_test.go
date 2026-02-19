package application

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServerCredentials_Disabled(t *testing.T) {
	t.Parallel()

	opt, err := ServerCredentials(TLSConfig{Enabled: false})
	require.NoError(t, err)
	require.Nil(t, opt)
}

func TestServerCredentials_Enabled(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	opt, err := ServerCredentials(TLSConfig{
		Enabled:  true,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
	})
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func TestServerCredentials_WithCA(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	opt, err := ServerCredentials(TLSConfig{
		Enabled:  true,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
		CAFile:   certs.CACertFile,
	})
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func TestServerCredentials_BadCertFile(t *testing.T) {
	t.Parallel()

	_, err := ServerCredentials(TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "loading server certificate")
}

func TestServerCredentials_BadCAFile(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	_, err := ServerCredentials(TLSConfig{
		Enabled:  true,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
		CAFile:   "/nonexistent/ca.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading CA certificate")
}

func TestClientTransportCredentials_Disabled(t *testing.T) {
	t.Parallel()

	creds, err := ClientTransportCredentials(TLSConfig{Enabled: false})
	require.NoError(t, err)
	require.IsType(t, insecure.NewCredentials(), creds)
}

func TestClientTransportCredentials_Enabled(t *testing.T) {
	t.Parallel()

	creds, err := ClientTransportCredentials(TLSConfig{Enabled: true})
	require.NoError(t, err)
	require.NotNil(t, creds)
	// Should NOT be insecure credentials
	require.NotEqual(t, "insecure", creds.Info().SecurityProtocol)
}

func TestClientTransportCredentials_WithCA(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	creds, err := ClientTransportCredentials(TLSConfig{
		Enabled: true,
		CAFile:  certs.CACertFile,
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.Equal(t, "tls", creds.Info().SecurityProtocol)
}

func TestClientTransportCredentials_WithClientCert(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	creds, err := ClientTransportCredentials(TLSConfig{
		Enabled:  true,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
		CAFile:   certs.CACertFile,
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.Equal(t, "tls", creds.Info().SecurityProtocol)
}

func TestClientTransportCredentials_BadCAFile(t *testing.T) {
	t.Parallel()

	_, err := ClientTransportCredentials(TLSConfig{
		Enabled: true,
		CAFile:  "/nonexistent/ca.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading CA certificate")
}

func TestClientTransportCredentials_BadClientCert(t *testing.T) {
	t.Parallel()

	_, err := ClientTransportCredentials(TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "loading client certificate")
}

// generateTestCerts generates test certificates in a temporary directory.
func generateTestCerts(t *testing.T) *testserver.TestCerts {
	t.Helper()
	certs, err := testserver.GenerateTestCerts(t.TempDir())
	require.NoError(t, err)
	return certs
}
