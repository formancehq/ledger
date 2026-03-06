package testserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// TestCerts holds the file paths for generated test certificates.
type TestCerts struct {
	CACertFile     string
	ServerCertFile string
	ServerKeyFile  string
}

// GenerateTestCerts creates a self-signed CA and a server certificate signed by that CA.
// Certificates are written to PEM files in the given directory.
// The server certificate is valid for localhost and 127.0.0.1.
func GenerateTestCerts(dir string) (*TestCerts, error) {
	// Generate CA key pair
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating CA key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("creating CA certificate: %w", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return nil, fmt.Errorf("parsing CA certificate: %w", err)
	}

	// Generate server key pair
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating server key: %w", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("creating server certificate: %w", err)
	}

	// Write CA cert
	caCertFile := filepath.Join(dir, "ca.pem")
	if err := writePEM(caCertFile, "CERTIFICATE", caCertDER); err != nil {
		return nil, fmt.Errorf("writing CA cert: %w", err)
	}

	// Write server cert
	serverCertFile := filepath.Join(dir, "server-cert.pem")
	if err := writePEM(serverCertFile, "CERTIFICATE", serverCertDER); err != nil {
		return nil, fmt.Errorf("writing server cert: %w", err)
	}

	// Write server key
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return nil, fmt.Errorf("marshaling server key: %w", err)
	}

	serverKeyFile := filepath.Join(dir, "server-key.pem")
	if err := writePEM(serverKeyFile, "EC PRIVATE KEY", serverKeyDER); err != nil {
		return nil, fmt.Errorf("writing server key: %w", err)
	}

	return &TestCerts{
		CACertFile:     caCertFile,
		ServerCertFile: serverCertFile,
		ServerKeyFile:  serverKeyFile,
	}, nil
}

func writePEM(path, blockType string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer func() { _ = f.Close() }()

	return pem.Encode(f, &pem.Block{
		Type:  blockType,
		Bytes: data,
	})
}
