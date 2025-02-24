package testserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/httpclient"
	"github.com/formancehq/go-libs/v2/httpserver"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/cmd"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/stretchr/testify/require"
)

type WorkerConfiguration struct {
	LogsHashBlockMaxSize  int
	LogsHashBlockCRONSpec string
}

func (cfg WorkerConfiguration) computeFlags() []string {
	args := make([]string, 0)
	if cfg.LogsHashBlockMaxSize > 0 {
		args = append(args, "--"+cmd.WorkerAsyncBlockHasherMaxBlockSizeFlag, strconv.Itoa(cfg.LogsHashBlockMaxSize))
	}
	if cfg.LogsHashBlockCRONSpec != "" {
		args = append(args, "--"+cmd.WorkerAsyncBlockHasherScheduleFlag, cfg.LogsHashBlockCRONSpec)
	}

	return args
}

type WorkerServiceConfiguration struct {
	CommonConfiguration
	WorkerConfiguration
	Output io.Writer
}

type Worker struct {
	configuration WorkerServiceConfiguration
	logger        Logger
	sdkClient     *ledgerclient.Formance
	cancel        func()
	ctx           context.Context
	errorChan     chan error
	id            string
	httpClient    *http.Client
	serverURL     string
}

func (s *Worker) Start() error {
	rootCmd := cmd.NewRootCommand()
	args := []string{
		"worker",
	}
	args = append(args, s.configuration.computeCommonFlags()...)
	args = append(args, s.configuration.WorkerConfiguration.computeFlags()...)

	s.logger.Logf("Starting application with flags: %s", strings.Join(args, " "))
	rootCmd.SetArgs(args)
	rootCmd.SilenceErrors = true
	output := s.configuration.Output
	if output == nil {
		output = io.Discard
	}
	rootCmd.SetOut(output)
	rootCmd.SetErr(output)

	ctx := logging.TestingContext()
	ctx = service.ContextWithLifecycle(ctx)
	ctx = httpserver.ContextWithServerInfo(ctx)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		s.errorChan <- rootCmd.ExecuteContext(ctx)
	}()

	select {
	case <-service.Ready(ctx):
	case err := <-s.errorChan:
		cancel()
		if err != nil {
			return err
		}

		return errors.New("unexpected service stop")
	}

	s.ctx, s.cancel = ctx, cancel

	var transport http.RoundTripper = &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
	}
	if s.configuration.Debug {
		transport = httpclient.NewDebugHTTPTransport(transport)
	}

	s.httpClient = &http.Client{
		Transport: transport,
	}
	s.serverURL = httpserver.URL(s.ctx)

	s.sdkClient = ledgerclient.New(
		ledgerclient.WithServerURL(s.serverURL),
		ledgerclient.WithClient(s.httpClient),
	)

	return nil
}

func (s *Worker) Stop(ctx context.Context) error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	s.cancel = nil

	// Wait app to be marked as stopped
	select {
	case <-service.Stopped(s.ctx):
	case <-ctx.Done():
		return errors.New("service should have been stopped")
	}

	// Ensure the app has been properly shutdown
	select {
	case err := <-s.errorChan:
		return err
	case <-ctx.Done():
		return errors.New("service should have been stopped without error")
	}
}

func (s *Worker) Client() *ledgerclient.Formance {
	return s.sdkClient
}

func (s *Worker) HTTPClient() *http.Client {
	return s.httpClient
}

func (s *Worker) ServerURL() string {
	return s.serverURL
}

func (s *Worker) Restart(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}

	return nil
}

func (s *Worker) Database() (*bun.DB, error) {
	db, err := bunconnect.OpenSQLDB(s.ctx, s.configuration.PostgresConfiguration)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *Worker) URL() string {
	return httpserver.URL(s.ctx)
}

func NewWorker(t T, configuration WorkerServiceConfiguration) *Worker {
	t.Helper()

	srv := &Worker{
		logger:        t,
		configuration: configuration,
		id:            uuid.NewString()[:8],
		errorChan:     make(chan error, 1),
	}
	t.Logf("Start testing server")
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		t.Logf("Stop testing server")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, srv.Stop(ctx))
	})

	return srv
}
