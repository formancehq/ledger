package testserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/publish"
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

type T interface {
	require.TestingT
	Cleanup(func())
	Helper()
	Logf(format string, args ...any)
}

type OTLPConfig struct {
	BaseConfig otlp.Config
	Metrics    *otlpmetrics.ModuleConfig
}

type CommonConfiguration struct {
	PostgresConfiguration bunconnect.ConnectionOptions
	Output                io.Writer
	Debug                 bool
	OTLPConfig            *OTLPConfig
}

func (cfg CommonConfiguration) computeCommonFlags() []string {
	args := []string{
		"--" + bunconnect.PostgresURIFlag, cfg.PostgresConfiguration.DatabaseSourceName,
	}
	if cfg.PostgresConfiguration.MaxIdleConns != 0 {
		args = append(
			args,
			"--"+bunconnect.PostgresMaxIdleConnsFlag,
			fmt.Sprint(cfg.PostgresConfiguration.MaxIdleConns),
		)
	}
	if cfg.PostgresConfiguration.MaxOpenConns != 0 {
		args = append(
			args,
			"--"+bunconnect.PostgresMaxOpenConnsFlag,
			fmt.Sprint(cfg.PostgresConfiguration.MaxOpenConns),
		)
	}
	if cfg.PostgresConfiguration.ConnMaxIdleTime != 0 {
		args = append(
			args,
			"--"+bunconnect.PostgresConnMaxIdleTimeFlag,
			fmt.Sprint(cfg.PostgresConfiguration.ConnMaxIdleTime),
		)
	}
	if cfg.OTLPConfig != nil {
		if cfg.OTLPConfig.Metrics != nil {
			args = append(
				args,
				"--"+otlpmetrics.OtelMetricsExporterFlag, cfg.OTLPConfig.Metrics.Exporter,
			)
			if cfg.OTLPConfig.Metrics.KeepInMemory {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsKeepInMemoryFlag,
				)
			}
			if cfg.OTLPConfig.Metrics.OTLPConfig != nil {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsExporterOTLPEndpointFlag, cfg.OTLPConfig.Metrics.OTLPConfig.Endpoint,
					"--"+otlpmetrics.OtelMetricsExporterOTLPModeFlag, cfg.OTLPConfig.Metrics.OTLPConfig.Mode,
				)
				if cfg.OTLPConfig.Metrics.OTLPConfig.Insecure {
					args = append(args, "--"+otlpmetrics.OtelMetricsExporterOTLPInsecureFlag)
				}
			}
			if cfg.OTLPConfig.Metrics.RuntimeMetrics {
				args = append(args, "--"+otlpmetrics.OtelMetricsRuntimeFlag)
			}
			if cfg.OTLPConfig.Metrics.MinimumReadMemStatsInterval != 0 {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsRuntimeMinimumReadMemStatsIntervalFlag,
					cfg.OTLPConfig.Metrics.MinimumReadMemStatsInterval.String(),
				)
			}
			if cfg.OTLPConfig.Metrics.PushInterval != 0 {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsExporterPushIntervalFlag,
					cfg.OTLPConfig.Metrics.PushInterval.String(),
				)
			}
			if len(cfg.OTLPConfig.Metrics.ResourceAttributes) > 0 {
				args = append(
					args,
					"--"+otlp.OtelResourceAttributesFlag,
					strings.Join(cfg.OTLPConfig.Metrics.ResourceAttributes, ","),
				)
			}
		}
		if cfg.OTLPConfig.BaseConfig.ServiceName != "" {
			args = append(args, "--"+otlp.OtelServiceNameFlag, cfg.OTLPConfig.BaseConfig.ServiceName)
		}
	}
	if cfg.Debug {
		args = append(args, "--"+service.DebugFlag)
	}

	return args
}

type Configuration struct {
	CommonConfiguration
	NatsURL                      string
	ExperimentalFeatures         bool
	DisableAutoUpgrade           bool
	BulkMaxSize                  int
	ExperimentalNumscriptRewrite bool
	MaxPageSize                  uint64
	DefaultPageSize              uint64
	WorkerEnabled                bool
	WorkerConfiguration          *WorkerConfiguration
}

type Logger interface {
	Logf(fmt string, args ...any)
}

type Server struct {
	configuration Configuration
	logger        Logger
	sdkClient     *ledgerclient.SDK
	cancel        func()
	ctx           context.Context
	errorChan     chan error
	id            string
	httpClient    *http.Client
	serverURL     string
}

func (s *Server) Start() error {
	rootCmd := cmd.NewRootCommand()
	args := []string{
		"serve",
		"--" + cmd.BindFlag, ":0",
	}
	args = append(args, s.configuration.computeCommonFlags()...)
	if !s.configuration.DisableAutoUpgrade {
		args = append(args, "--"+cmd.AutoUpgradeFlag)
	}
	if s.configuration.WorkerEnabled {
		args = append(args, "--"+cmd.WorkerEnabledFlag)
		if s.configuration.WorkerConfiguration != nil {
			args = append(args, s.configuration.WorkerConfiguration.computeFlags()...)
		}
	}
	if s.configuration.ExperimentalFeatures {
		args = append(
			args,
			"--"+cmd.ExperimentalFeaturesFlag,
		)
	}
	if s.configuration.BulkMaxSize != 0 {
		args = append(
			args,
			"--"+cmd.BulkMaxSizeFlag,
			fmt.Sprint(s.configuration.BulkMaxSize),
		)
	}
	if s.configuration.ExperimentalNumscriptRewrite {
		args = append(
			args,
			"--"+cmd.NumscriptInterpreterFlag,
		)
	}
	if s.configuration.NatsURL != "" {
		args = append(
			args,
			"--"+publish.PublisherNatsEnabledFlag,
			"--"+publish.PublisherNatsURLFlag, s.configuration.NatsURL,
			"--"+publish.PublisherTopicMappingFlag, fmt.Sprintf("*:%s", s.id),
		)
	}
	if s.configuration.MaxPageSize != 0 {
		args = append(args, "--"+cmd.MaxPageSizeFlag, fmt.Sprint(s.configuration.MaxPageSize))
	}
	if s.configuration.DefaultPageSize != 0 {
		args = append(args, "--"+cmd.DefaultPageSizeFlag, fmt.Sprint(s.configuration.DefaultPageSize))
	}

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

func (s *Server) Stop(ctx context.Context) error {
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

func (s *Server) Client() *ledgerclient.SDK {
	return s.sdkClient
}

func (s *Server) HTTPClient() *http.Client {
	return s.httpClient
}

func (s *Server) ServerURL() string {
	return s.serverURL
}

func (s *Server) Restart(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}

	return nil
}

func (s *Server) Database() (*bun.DB, error) {
	db, err := bunconnect.OpenSQLDB(s.ctx, s.configuration.PostgresConfiguration)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *Server) Subscribe() (*nats.Subscription, chan *nats.Msg, error) {
	if s.configuration.NatsURL == "" {
		return nil, nil, errors.New("NATS URL must be set")
	}

	ret := make(chan *nats.Msg)
	conn, err := nats.Connect(s.configuration.NatsURL)
	if err != nil {
		return nil, nil, err
	}

	subscription, err := conn.Subscribe(s.id, func(msg *nats.Msg) {
		ret <- msg
	})
	if err != nil {
		return nil, nil, err
	}

	return subscription, ret, nil
}

func (s *Server) URL() string {
	return httpserver.URL(s.ctx)
}

func New(t T, configuration Configuration) *Server {
	t.Helper()

	srv := &Server{
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
