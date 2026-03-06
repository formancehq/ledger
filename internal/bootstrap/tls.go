package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ServerCredentials returns a grpc.ServerOption with TLS credentials when TLS is enabled,
// or nil when disabled. If CAFile is set, client certificate verification is configured
// with tls.VerifyClientCertIfGiven (opt-in mTLS).
func ServerCredentials(cfg TLSConfig) (grpc.ServerOption, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server certificate: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if cfg.CAFile != "" {
		caPool, err := loadCACertPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		tlsCfg.ClientCAs = caPool
		tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
	}

	return grpc.Creds(credentials.NewTLS(tlsCfg)), nil
}

// ClientTransportCredentials returns transport credentials for gRPC client connections.
// When TLS is disabled, it returns insecure credentials.
// When enabled, it loads the CA pool from CAFile (if set) and client cert/key (if set) for mTLS.
func ClientTransportCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	if !cfg.Enabled {
		return insecure.NewCredentials(), nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cfg.CAFile != "" {
		caPool, err := loadCACertPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		tlsCfg.RootCAs = caPool
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}

		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsCfg), nil
}

// loadCACertPool reads a PEM-encoded CA certificate file and returns an x509.CertPool.
func loadCACertPool(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse CA certificate from %s", caFile)
	}

	return pool, nil
}
