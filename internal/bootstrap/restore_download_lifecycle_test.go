//go:build s3

package bootstrap

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/pkg/network"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func TestRestoreDownloadStopsWithFxApplication(t *testing.T) {
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
	app := fx.New(
		fx.NopLogger,
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
		_ = app.Stop(context.Background())
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
	<-requestStarted

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

	_, err = restoreServer.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotEmpty(t, start.GetJobId())
}
