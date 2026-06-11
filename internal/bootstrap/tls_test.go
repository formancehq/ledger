package bootstrap

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/v3/pkg/testserver"
)

func TestServerTLSConfig_Disabled(t *testing.T) {
	t.Parallel()

	cfg, reloader, err := ServerTLSConfig(TLSConfig{Mode: TLSModeDisabled})
	require.NoError(t, err)
	require.Nil(t, cfg)
	require.Nil(t, reloader)
}

func TestServerTLSConfig_Required(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	cfg, reloader, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, reloader)
	// Certificates are served lazily via GetCertificate, not pre-listed.
	require.Nil(t, cfg.Certificates)
	require.NotNil(t, cfg.GetCertificate)

	got, err := cfg.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, got.Certificate)
}

func TestServerTLSConfig_Optional(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	cfg, reloader, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeOptional,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, reloader)
	require.NotNil(t, cfg.GetCertificate)
}

func TestServerTLSConfig_WithCA_VerifyIfGiven(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	cfg, _, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: certs.ServerCertFile,
		KeyFile:  certs.ServerKeyFile,
		CAFile:   certs.CACertFile,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.GetConfigForClient)

	perConn, err := cfg.GetConfigForClient(nil)
	require.NoError(t, err)
	require.NotNil(t, perConn.ClientCAs)
	require.Equal(t, tls.VerifyClientCertIfGiven, perConn.ClientAuth,
		"default posture must keep the historical VerifyClientCertIfGiven behavior")
}

func TestServerTLSConfig_WithCA_RequireClientCert(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	cfg, _, err := ServerTLSConfig(TLSConfig{
		Mode:              TLSModeRequired,
		CertFile:          certs.ServerCertFile,
		KeyFile:           certs.ServerKeyFile,
		CAFile:            certs.CACertFile,
		RequireClientCert: true,
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.GetConfigForClient)

	perConn, err := cfg.GetConfigForClient(nil)
	require.NoError(t, err)
	require.NotNil(t, perConn.ClientCAs)
	require.Equal(t, tls.RequireAndVerifyClientCert, perConn.ClientAuth,
		"opt-in posture must enforce client cert presence at the TLS layer")
}

func TestServerTLSConfig_BadCertFile(t *testing.T) {
	t.Parallel()

	_, _, err := ServerTLSConfig(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "loading certificate")
}

func TestServerTLSConfig_BadCAFile(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	_, _, err := ServerTLSConfig(TLSConfig{
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

	creds, reloader, err := ClientTransportCredentials(TLSConfig{Mode: TLSModeDisabled})
	require.NoError(t, err)
	require.IsType(t, insecure.NewCredentials(), creds)
	require.Nil(t, reloader)
}

func TestClientTransportCredentials_Required(t *testing.T) {
	t.Parallel()

	creds, reloader, err := ClientTransportCredentials(TLSConfig{Mode: TLSModeRequired})
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.NotNil(t, reloader)
	require.NotEqual(t, "insecure", creds.Info().SecurityProtocol)
}

func TestClientTransportCredentials_WithCA(t *testing.T) {
	t.Parallel()

	certs := generateTestCerts(t)

	creds, _, err := ClientTransportCredentials(TLSConfig{
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

	creds, _, err := ClientTransportCredentials(TLSConfig{
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

	_, _, err := ClientTransportCredentials(TLSConfig{
		Mode:   TLSModeRequired,
		CAFile: "/nonexistent/ca.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading CA certificate")
}

func TestClientTransportCredentials_BadClientCert(t *testing.T) {
	t.Parallel()

	_, _, err := ClientTransportCredentials(TLSConfig{
		Mode:     TLSModeRequired,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "loading certificate")
}

func TestClientTLSConfig_Disabled(t *testing.T) {
	t.Parallel()

	cfg, reloader, err := ClientTLSConfig(TLSConfig{Mode: TLSModeDisabled})
	require.NoError(t, err)
	require.Nil(t, cfg)
	require.Nil(t, reloader)
}

func TestClientTLSConfig_Optional(t *testing.T) {
	t.Parallel()

	cfg, _, err := ClientTLSConfig(TLSConfig{Mode: TLSModeOptional})
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
