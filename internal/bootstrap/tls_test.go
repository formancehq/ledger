package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/v3/pkg/testserver"
)

func TestServerTLSConfig_Disabled(t *testing.T) {
	t.Parallel()

	cfg, err := ServerTLSConfig(TLSConfig{Mode: TLSModeDisabled})
	require.NoError(t, err)
	require.Nil(t, cfg)
}

func TestServerTLSConfig_Required(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	cfg, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Len(t, cfg.Certificates, 1)
}

func TestServerTLSConfig_Optional(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	cfg, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeOptional,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Len(t, cfg.Certificates, 1)
}

func TestServerTLSConfig_WithCA(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	cfg, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
		CAFile:   certs.CACertFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.ClientCAs)
}

func TestServerTLSConfig_BadCertFile(t *testing.T) {
	t.Parallel()

	_, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "loading server certificate")
}

func TestServerTLSConfig_BadCAFile(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	_, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
		CAFile:   "/nonexistent/ca.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading CA certificate")
}

func TestClientTransportCredentials_Disabled(t *testing.T) {
	t.Parallel()

	creds, err := ClientTransportCredentials(TLSConfig{Mode: TLSModeDisabled})
	require.NoError(t, err)
	require.IsType(t, insecure.NewCredentials(), creds)
}

func TestClientTransportCredentials_Required(t *testing.T) {
	t.Parallel()

	creds, err := ClientTransportCredentials(TLSConfig{Mode: TLSModeRequired})
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.NotEqual(t, "insecure", creds.Info().SecurityProtocol)
}

func TestClientTransportCredentials_WithCA(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	creds, err := ClientTransportCredentials(TLSConfig{
		Mode:   TLSModeRequired,
		CAFile: certs.CACertFile,
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.Equal(t, "tls", creds.Info().SecurityProtocol)
}

func TestClientTransportCredentials_WithClientCert(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	creds, err := ClientTransportCredentials(TLSConfig{
		Mode:     TLSModeRequired,
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
		Mode:   TLSModeRequired,
		CAFile: "/nonexistent/ca.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading CA certificate")
}

func TestClientTransportCredentials_BadClientCert(t *testing.T) {
	t.Parallel()

	_, err := ClientTransportCredentials(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "loading client certificate")
}

func TestClientTLSConfig_Disabled(t *testing.T) {
	t.Parallel()

	cfg, err := ClientTLSConfig(TLSConfig{Mode: TLSModeDisabled})
	require.NoError(t, err)
	require.Nil(t, cfg)
}

func TestClientTLSConfig_Optional(t *testing.T) {
	t.Parallel()

	cfg, err := ClientTLSConfig(TLSConfig{Mode: TLSModeOptional})
	require.NoError(t, err)
	require.NotNil(t, cfg)
}

// generateTestCerts generates test certificates in a temporary directory.
func generateTestCerts(t *testing.T) *testserver.TestCerts {
	t.Helper()
	certs, err := testserver.GenerateTestCerts(t.TempDir())
	require.NoError(t, err)

	return certs
}
