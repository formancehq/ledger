package bootstrap

import (
	"crypto/tls"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ServerTLSConfig returns the *tls.Config for inter-node and service gRPC
// servers, plus a CertReloader that watches the configured files for
// rotations. Returns (nil, nil, nil) when TLS is disabled.
//
// The returned *tls.Config installs GetCertificate (server keypair) and, when
// a CA is configured, GetConfigForClient (client trust pool + ClientAuth
// posture) so that cert-manager rotations are picked up without a restart.
// When TLSConfig.RequireClientCert is set, peers that do not present a
// CA-signed certificate are rejected at the TLS layer.
//
// The caller owns the CertReloader's lifecycle: Start it on a long-running
// context to enable fs-watch + polling, and Stop it on shutdown. Short-lived
// callers (CLIs, tests) may simply ignore it — the reloader's initial load
// already populated the callbacks.
func ServerTLSConfig(cfg TLSConfig) (*tls.Config, *CertReloader, error) {
	if cfg.Mode == TLSModeDisabled {
		return nil, nil, nil
	}

	reloader, err := NewCertReloader(cfg)
	if err != nil {
		return nil, nil, err
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// gRPC requires h2 ALPN. credentials.NewTLS sets this implicitly,
		// but we expose the raw *tls.Config to cmux + tls.NewListener and
		// must set it ourselves.
		NextProtos:     []string{"h2"},
		GetCertificate: reloader.GetCertificate,
	}

	if cfg.CAFile != "" {
		clientAuth := tls.VerifyClientCertIfGiven
		if cfg.RequireClientCert {
			clientAuth = tls.RequireAndVerifyClientCert
		}

		// GetConfigForClient is invoked per inbound handshake, so it picks
		// up CA pool reloads at the next connection without restarting the
		// listener. The cloned config inherits MinVersion / NextProtos /
		// GetCertificate.
		tlsCfg.GetConfigForClient = func(_ *tls.ClientHelloInfo) (*tls.Config, error) {
			perConn := tlsCfg.Clone()
			perConn.ClientCAs = reloader.ClientCAs()
			perConn.ClientAuth = clientAuth

			return perConn, nil
		}
	}

	return tlsCfg, reloader, nil
}

// ClientTLSConfig returns the *tls.Config for outbound inter-node dials and
// the associated CertReloader. Returns (nil, nil, nil) when TLS is disabled.
//
// What reloads live on outbound dials:
//
//   - **Client leaf cert / key** — handled via GetClientCertificate, which is
//     invoked by the TLS stack on every handshake and consults the reloader.
//
// What does NOT reload live on outbound dials:
//
//   - **RootCAs (server trust pool)** — tls.Config has no GetRootCAs callback.
//     credentials.NewTLS clones the config into the gRPC transport
//     credentials, so the *x509.CertPool we set here is frozen for the
//     lifetime of those credentials. CA bundle changes only take effect when
//     the process restarts (or when a future revision migrates to a custom
//     VerifyPeerCertificate that reads the live pool — left as follow-up).
//
// For the typical cert-manager deployment this is acceptable: leaf certs
// rotate every 60–90 days and ARE picked up live; CA bundle changes are rare
// and operator-orchestrated.
func ClientTLSConfig(cfg TLSConfig) (*tls.Config, *CertReloader, error) {
	if cfg.Mode == TLSModeDisabled {
		return nil, nil, nil
	}

	reloader, err := NewCertReloader(cfg)
	if err != nil {
		return nil, nil, err
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cfg.CAFile != "" {
		tlsCfg.RootCAs = reloader.RootCAs()
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsCfg.GetClientCertificate = reloader.GetClientCertificate
	}

	return tlsCfg, reloader, nil
}

// ClientTransportCredentials returns transport credentials suitable for the
// strict modes, along with the cert reloader. For TLSModeRequired it returns
// TLS credentials; for TLSModeDisabled it returns insecure credentials and a
// nil reloader. In TLSModeOptional callers must perform per-peer probing
// using ClientTLSConfig directly.
func ClientTransportCredentials(cfg TLSConfig) (credentials.TransportCredentials, *CertReloader, error) {
	if cfg.Mode == TLSModeDisabled {
		return insecure.NewCredentials(), nil, nil
	}

	tlsCfg, reloader, err := ClientTLSConfig(cfg)
	if err != nil {
		return nil, nil, err
	}

	return credentials.NewTLS(tlsCfg), reloader, nil
}
