package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
	"github.com/formancehq/ledger/v3/pkg/testserver"
)

func TestFailedOIDCStartupReleasesPebbleAndListeners(t *testing.T) {
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary network failure", http.StatusServiceUnavailable)
	}))
	t.Cleanup(issuer.Close)

	lease := testserver.AllocateNodeLease()
	dataDir, walDir := t.TempDir(), t.TempDir()
	certs, err := testserver.GenerateTestCerts(t.TempDir())
	require.NoError(t, err)
	var output strings.Builder
	instruments := append(testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "startup-cleanup-test", Ports: lease.Ports(),
		DataDir: dataDir, WalDir: walDir, Output: &output,
	}), testserver.WithBootstrap(), testserver.WithAuthEnabled(), testserver.WithAuthIssuer(issuer.URL), testserver.WithAuthService("ledger"),
		testserver.WithTLSMode("required"), testserver.WithTLSCertFile(certs.ServerCertFile),
		testserver.WithTLSKeyFile(certs.ServerKeyFile), testserver.WithTLSCACertFile(certs.CACertFile))
	server := lease.NewService(NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	err = server.Start(logging.TestingContext())
	require.ErrorContains(t, err, "discovering OIDC configuration")
	require.Contains(t, output.String(), "Pebble database opened")

	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(dataDir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err, "failed startup must release the Pebble lock")
	require.NoError(t, store.Close())
	w, err := wal.New(walDir, logger, meter)
	require.NoError(t, err, "failed startup must release the Raft WAL")
	require.NoError(t, w.Close())
	rs, err := readstore.New(filepath.Join(dataDir, "read-indexes"), logger, readstore.DefaultConfig())
	require.NoError(t, err, "failed startup must release the read store")
	require.NoError(t, rs.Close())
	us, err := usagestore.New(filepath.Join(dataDir, "usage"), logger, usagestore.DefaultConfig())
	require.NoError(t, err, "failed startup must release the usage store")
	require.NoError(t, us.Close())
	sp, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: filepath.Join(dataDir, "spool"), Logger: logger})
	require.NoError(t, err, "failed startup must release the spool")
	require.NoError(t, sp.Close())

	bindings := lease.NextGeneration()
	for _, listener := range []interface{ Close() error }{bindings.HTTP, bindings.Service, bindings.Raft} {
		require.NoError(t, listener.Close())
	}
}

func TestConfigFailureReleasesInjectedListeners(t *testing.T) {
	lease := testserver.AllocateNodeLease()
	bindings := lease.NextGeneration()
	command := NewRunCommandWithBindings(bindings)
	command.SetArgs([]string{
		"--node-id=1", "--cluster-id=startup-cleanup-test", "--bootstrap",
		"--data-dir=" + t.TempDir(), "--wal-dir=" + t.TempDir(),
		"--bind-addr=" + bindings.Raft.Addr().String(),
		"--auth-enabled", "--auth-issuer=https://issuer.example",
	})
	require.ErrorContains(t, command.Execute(), "--tls-mode=required")

	next := lease.NextGeneration()
	for _, listener := range []interface{ Close() error }{next.HTTP, next.Service, next.Raft} {
		require.NoError(t, listener.Close())
	}
}
