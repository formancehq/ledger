package testserver

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateTestCerts(t *testing.T) {
	t.Parallel()

	certs, err := GenerateTestCerts(t.TempDir())
	require.NoError(t, err)

	// Verify files exist
	for _, path := range []string{certs.CACertFile, certs.ServerCertFile, certs.ServerKeyFile} {
		_, err := os.Stat(path)
		require.NoError(t, err, "file should exist: %s", path)
	}

	// Verify CA cert is parseable
	caPEM, err := os.ReadFile(certs.CACertFile)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	require.True(t, caPool.AppendCertsFromPEM(caPEM), "CA cert should be valid PEM")

	// Verify server cert + key pair loads
	_, err = tls.LoadX509KeyPair(certs.ServerCertFile, certs.ServerKeyFile)
	require.NoError(t, err, "server cert/key pair should load")
}
