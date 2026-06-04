package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ServerTLSConfig returns the *tls.Config for inter-node and service gRPC
// servers. Returns nil when TLS is disabled. When CAFile is set, opt-in mTLS
// is enabled via VerifyClientCertIfGiven.
//
// The raw *tls.Config (rather than a grpc.ServerOption) lets callers wrap a
// net.Listener with tls.NewListener and/or share the config across two
// servers in the optional dual-listener mode.
func ServerTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if cfg.Mode == TLSModeDisabled {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server certificate: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		// gRPC requires h2 ALPN. credentials.NewTLS sets this implicitly,
		// but we expose the raw *tls.Config to cmux + tls.NewListener and
		// must set it ourselves.
		NextProtos: []string{"h2"},
	}

	if cfg.CAFile != "" {
		caPool, err := loadCACertPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		tlsCfg.ClientCAs = caPool
		tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
	}

	return tlsCfg, nil
}

// ClientTLSConfig returns the *tls.Config for outbound inter-node dials.
// Returns nil when TLS is disabled. When CertFile/KeyFile are set, the
// client presents a certificate (mTLS).
func ClientTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if cfg.Mode == TLSModeDisabled {
		return nil, nil
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

	return tlsCfg, nil
}

// ClientTransportCredentials returns transport credentials suitable for the
// strict modes. For TLSModeRequired it returns TLS credentials; for
// TLSModeDisabled it returns insecure credentials. In TLSModeOptional callers
// must perform per-peer probing using ClientTLSConfig directly.
func ClientTransportCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	if cfg.Mode == TLSModeDisabled {
		return insecure.NewCredentials(), nil
	}

	tlsCfg, err := ClientTLSConfig(cfg)
	if err != nil {
		return nil, err
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
