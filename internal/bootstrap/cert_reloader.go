package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// certReloaderPollInterval is the fallback TTL between mtime checks when
// fsnotify either misses an event or is unavailable. Kubernetes mounts
// ConfigMaps and Secrets as symlinks pointing into a `..data` directory that
// is atomically swapped on update; not every fsnotify backend forwards those
// rename events on every kernel, so we keep a coarse polling loop as a
// belt-and-braces fallback. Cert rotations are rare (typically every 60–90
// days), so a one-minute lag is well within the operational envelope.
//
// Variable rather than const so tests can shrink it; production code never
// reassigns it.
var certReloaderPollInterval = time.Minute

// CertReloader keeps the current TLS server key pair, client key pair, and
// trust pool in memory and refreshes them when the backing files change. It
// is wired into tls.Config via GetCertificate / GetClientCertificate /
// GetConfigForClient callbacks, so a long-running process picks up
// cert-manager rotations without a restart.
//
// Lifecycle:
//
//   - NewCertReloader(cfg) performs the initial load and returns a ready-to-use
//     instance even if Start is never called (used by short-lived CLIs and
//     tests, which don't need background reloading).
//   - Start(ctx, logger) launches the fsnotify watcher and a polling fallback.
//     The watcher reloads on any write/rename event on the cert, key, or CA
//     files (or on their parent directory, to track atomic K8s swaps).
//   - Stop() drains the watcher; Start/Stop are idempotent and safe to call
//     in any order.
type CertReloader struct {
	cfg TLSConfig

	// Atomic pointers so GetCertificate/GetClientCertificate are lock-free.
	serverCert atomic.Pointer[tls.Certificate]
	clientCert atomic.Pointer[tls.Certificate]
	caPool     atomic.Pointer[x509.CertPool]

	// caPEM is the last bytes loaded from CAFile, used to skip a CertPool
	// rebuild when the file content hasn't changed.
	caMu  sync.Mutex
	caPEM []byte

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
}

// NewCertReloader constructs a reloader and performs the initial load for the
// configured paths. Empty paths are simply skipped, which mirrors the
// pre-existing behavior of ServerTLSConfig / ClientTLSConfig — for example,
// a server with CAFile unset has no client trust pool.
func NewCertReloader(cfg TLSConfig) (*CertReloader, error) {
	r := &CertReloader{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}

	if err := r.reload(); err != nil {
		return nil, err
	}

	return r, nil
}

// reload re-reads all configured files and atomically swaps the cached values.
// Partial failures (e.g. a stale file written halfway through) are reported
// but do not replace the previously cached values, so the cluster keeps
// serving the old (still-valid) cert until rotation completes.
func (r *CertReloader) reload() error {
	if r.cfg.CertFile != "" && r.cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(r.cfg.CertFile, r.cfg.KeyFile)
		if err != nil {
			return fmt.Errorf("loading certificate: %w", err)
		}

		r.serverCert.Store(&cert)
		r.clientCert.Store(&cert)
	}

	if r.cfg.CAFile != "" {
		caPEM, err := os.ReadFile(r.cfg.CAFile)
		if err != nil {
			return fmt.Errorf("reading CA certificate: %w", err)
		}

		r.caMu.Lock()
		unchanged := len(r.caPEM) == len(caPEM) && string(r.caPEM) == string(caPEM)

		if !unchanged {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				r.caMu.Unlock()

				return fmt.Errorf("failed to parse CA certificate from %s", r.cfg.CAFile)
			}

			r.caPool.Store(pool)
			r.caPEM = caPEM
		}
		r.caMu.Unlock()
	}

	return nil
}

// GetCertificate is the tls.Config.GetCertificate callback for server-side
// handshakes. It returns the latest server certificate loaded from disk.
func (r *CertReloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	cert := r.serverCert.Load()
	if cert == nil {
		return nil, errors.New("no server certificate loaded")
	}

	return cert, nil
}

// GetClientCertificate is the tls.Config.GetClientCertificate callback for
// client-side handshakes. Returns an empty certificate (no client auth) when
// no key pair is configured — the caller-side guard, ClientTLSConfig, only
// installs this hook when CertFile/KeyFile are set, so the empty case is
// effectively unreachable in production but matters for tests.
func (r *CertReloader) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cert := r.clientCert.Load()
	if cert == nil {
		return &tls.Certificate{}, nil
	}

	return cert, nil
}

// ClientCAs returns the current CA trust pool, or nil when no CA was configured.
func (r *CertReloader) ClientCAs() *x509.CertPool {
	return r.caPool.Load()
}

// RootCAs is an alias for ClientCAs — semantically the same x509.CertPool is
// used for both inbound (ClientCAs) and outbound (RootCAs) trust in this
// codebase, since cluster nodes share a single CA bundle.
func (r *CertReloader) RootCAs() *x509.CertPool {
	return r.caPool.Load()
}

// Start launches the fsnotify watcher and the polling fallback. It is
// idempotent: only the first call wires the goroutines; subsequent calls are
// no-ops. Safe to skip entirely (CLIs, tests) — GetCertificate keeps working
// against the initial load.
func (r *CertReloader) Start(ctx context.Context, logger logging.Logger) {
	r.startOnce.Do(func() {
		go r.run(ctx, logger)
	})
}

// RegisterCertReloaderLifecycle wires a reloader's Start/Stop into an fx
// Lifecycle. Safe to call with a nil reloader (e.g. TLS disabled): it's a
// no-op in that case. Pulled out to keep the bootstrap fx providers thin.
func RegisterCertReloaderLifecycle(lc fx.Lifecycle, r *CertReloader, logger logging.Logger) {
	if r == nil {
		return
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			r.Start(ctx, logger)

			return nil
		},
		OnStop: func(_ context.Context) error {
			r.Stop()

			return nil
		},
	})
}

// Stop signals the background watcher to exit. Idempotent.
//
// Stop returns immediately — it does not wait for the goroutine to finish
// closing the fsnotify handle. fx Lifecycle teardown is async by nature and
// the process is on its way out anyway, so a brief overlap is harmless. For
// the small subset of tests that need synchronization, leak detection is the
// right tool.
func (r *CertReloader) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}

// run is the background reload loop. It watches every configured file path
// (and its parent directory, to catch atomic Kubernetes swaps that rename
// `..data` rather than writing in place) plus a polling tick for kernels and
// filesystems where fsnotify is unreliable.
func (r *CertReloader) run(ctx context.Context, logger logging.Logger) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Errorf("cert reloader: fsnotify unavailable, falling back to polling only: %v", err)

		r.pollLoop(ctx, logger)

		return
	}

	defer func() { _ = watcher.Close() }()

	for _, p := range r.watchPaths() {
		if err := watcher.Add(p); err != nil {
			// Watching the parent directory is best-effort; an unreadable
			// parent does not prevent us from reloading via the poll tick.
			logger.Infof("cert reloader: cannot watch %s: %v (poll fallback remains active)", p, err)
		}
	}

	ticker := time.NewTicker(certReloaderPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.reload(); err != nil {
				logger.Errorf("cert reloader: poll reload failed: %v", err)
			}
		case ev, ok := <-watcher.Events:
			if !ok {
				return
			}

			if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
				continue
			}

			if err := r.reload(); err != nil {
				logger.Errorf("cert reloader: fsnotify reload failed: %v", err)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}

			logger.Infof("cert reloader: fsnotify error: %v", err)
		}
	}
}

// pollLoop runs the TTL fallback alone — used when fsnotify cannot be created
// (e.g. inotify watch count exhausted on the host).
func (r *CertReloader) pollLoop(ctx context.Context, logger logging.Logger) {
	ticker := time.NewTicker(certReloaderPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.reload(); err != nil {
				logger.Errorf("cert reloader: poll reload failed: %v", err)
			}
		}
	}
}

// watchPaths returns the set of files and parent directories to register with
// fsnotify. Watching the parents catches atomic-rename strategies (Kubernetes
// projects ConfigMap/Secret updates by swapping a `..data` symlink rather
// than touching the file the application sees).
func (r *CertReloader) watchPaths() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, 6)

	add := func(p string) {
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}

		seen[p] = struct{}{}
		out = append(out, p)
	}

	for _, p := range []string{r.cfg.CertFile, r.cfg.KeyFile, r.cfg.CAFile} {
		if p == "" {
			// Skip empty entries entirely; filepath.Dir("") would otherwise
			// return ".", causing us to register a watch on the process's
			// current working directory.
			continue
		}

		add(p)
		add(filepath.Dir(p))
	}

	return out
}
