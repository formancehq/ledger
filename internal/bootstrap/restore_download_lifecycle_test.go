//go:build s3

package bootstrap

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/pkg/network"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// restoreLifecycleEvents observes the hooks Fx actually executes. Method names
// identify the two restore phases without depending on anonymous closure numbers.
type restoreLifecycleEvents struct {
	mu        sync.Mutex
	functions []string
}

func (l *restoreLifecycleEvents) LogEvent(event fxevent.Event) {
	if event, ok := event.(*fxevent.OnStopExecuting); ok {
		l.mu.Lock()
		defer l.mu.Unlock()

		l.functions = append(l.functions, event.FunctionName)
	}
}

func (l *restoreLifecycleEvents) stopPhases() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	var phases []string
	for _, name := range l.functions {
		switch {
		case strings.Contains(name, "RestoreServiceServerImpl).BeginShutdown"):
			phases = append(phases, "close admission and cancel download")
		case strings.Contains(name, ".httpServerHook."):
			phases = append(phases, "stop HTTP")
		case strings.Contains(name, ".grpcServerHook."):
			phases = append(phases, "stop gRPC")
		case strings.Contains(name, "RestoreServiceServerImpl).Shutdown"):
			phases = append(phases, "join restore requests and job")
		}
	}

	return phases
}

func TestRestoreDownloadStopsWithFxApplication(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	releaseRequest := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(requestStarted) })
		select {
		case <-r.Context().Done():
			canceledOnce.Do(func() { close(requestCanceled) })
		case <-releaseRequest:
			http.Error(w, "released", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(func() {
		select {
		case <-releaseRequest:
		default:
			close(releaseRequest)
		}
		backend.Close()
	})

	httpListener := mustListenLoopback(t, 0)
	serviceListener := mustListenLoopback(t, 0)
	cfg := Config{
		ClusterID:     "restore-lifecycle-before-fix",
		DataDir:       t.TempDir(),
		Restore:       true,
		RestoreListen: "127.0.0.1",
		HTTPPort:      httpListener.Addr().(*net.TCPAddr).Port,
		GRPCPort:      serviceListener.Addr().(*net.TCPAddr).Port,
		TLSConfig:     TLSConfig{Mode: TLSModeDisabled},
	}

	var restoreServer *grpcadp.RestoreServiceServerImpl
	lifecycleEvents := &restoreLifecycleEvents{}
	app := fx.New(
		fx.WithLogger(func() fxevent.Logger { return lifecycleEvents }),
		fx.Supply(cfg),
		fx.Supply(network.Bindings{HTTP: httpListener, Service: serviceListener}),
		fx.Provide(func() logging.Logger { return logging.Testing() }),
		RestoreModule(),
		fx.Populate(&restoreServer),
	)
	require.NoError(t, app.Err())
	t.Cleanup(func() {
		select {
		case <-releaseRequest:
		default:
			close(releaseRequest)
		}
		require.NoError(t, app.Stop(context.Background()))
	})

	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()
	require.NoError(t, app.Start(startCtx))

	start, err := restoreServer.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{
		Storage: &commonpb.BackupStorage{Provider: &commonpb.BackupStorage_S3{S3: &commonpb.S3StorageConfig{
			Bucket:          "backups",
			Region:          "us-east-1",
			Endpoint:        backend.URL,
			AccessKeyId:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		}}},
	})
	require.NoError(t, err)
	select {
	case <-requestStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("restore job did not reach the S3 backend")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- app.Stop(context.Background()) }()

	select {
	case <-requestCanceled:
	case <-time.After(10 * time.Second):
		t.Fatal("Fx shutdown did not cancel the restore job")
	}

	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Fx shutdown did not join the canceled restore job")
	}

	// Event order comes from Fx's synchronous hook execution, not goroutine
	// scheduling. Cancellation alone could pass with either restore hook missing
	// or with their positions swapped, because Shutdown also calls BeginShutdown.
	require.Equal(t, []string{
		"close admission and cancel download",
		"stop HTTP",
		"stop gRPC",
		"join restore requests and job",
	}, lifecycleEvents.stopPhases(), "restore shutdown phases must surround network teardown")

	_, err = restoreServer.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotEmpty(t, start.GetJobId())
}
