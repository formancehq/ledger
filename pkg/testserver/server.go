package testserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/publish"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/httpclient"
	"github.com/formancehq/go-libs/v2/httpserver"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/cmd"
	ledgerclient "github.com/formancehq/stack/ledger/client"
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

type Configuration struct {
	PostgresConfiguration        bunconnect.ConnectionOptions
	NatsURL                      string
	Output                       io.Writer
	Debug                        bool
	OTLPConfig                   *OTLPConfig
	ExperimentalFeatures         bool
	BulkMaxSize                  int
	ExperimentalNumscriptRewrite bool
}

type Server struct {
	configuration Configuration
	t             T
	httpClient    *ledgerclient.Formance
	cancel        func()
	ctx           context.Context
	errorChan     chan error
	id            string
}

func (s *Server) Start() {
	s.t.Helper()

	rootCmd := cmd.NewRootCommand()
	args := []string{
		"serve",
		"--" + cmd.BindFlag, ":0",
		"--" + cmd.AutoUpgradeFlag,
		"--" + bunconnect.PostgresURIFlag, s.configuration.PostgresConfiguration.DatabaseSourceName,
		"--" + bunconnect.PostgresMaxOpenConnsFlag, fmt.Sprint(s.configuration.PostgresConfiguration.MaxOpenConns),
		"--" + bunconnect.PostgresConnMaxIdleTimeFlag, fmt.Sprint(s.configuration.PostgresConfiguration.ConnMaxIdleTime),
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
	if s.configuration.PostgresConfiguration.MaxIdleConns != 0 {
		args = append(
			args,
			"--"+bunconnect.PostgresMaxIdleConnsFlag,
			fmt.Sprint(s.configuration.PostgresConfiguration.MaxIdleConns),
		)
	}
	if s.configuration.PostgresConfiguration.MaxOpenConns != 0 {
		args = append(
			args,
			"--"+bunconnect.PostgresMaxOpenConnsFlag,
			fmt.Sprint(s.configuration.PostgresConfiguration.MaxOpenConns),
		)
	}
	if s.configuration.PostgresConfiguration.ConnMaxIdleTime != 0 {
		args = append(
			args,
			"--"+bunconnect.PostgresConnMaxIdleTimeFlag,
			fmt.Sprint(s.configuration.PostgresConfiguration.ConnMaxIdleTime),
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
	if s.configuration.OTLPConfig != nil {
		if s.configuration.OTLPConfig.Metrics != nil {
			args = append(
				args,
				"--"+otlpmetrics.OtelMetricsExporterFlag, s.configuration.OTLPConfig.Metrics.Exporter,
			)
			if s.configuration.OTLPConfig.Metrics.KeepInMemory {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsKeepInMemoryFlag,
				)
			}
			if s.configuration.OTLPConfig.Metrics.OTLPConfig != nil {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsExporterOTLPEndpointFlag, s.configuration.OTLPConfig.Metrics.OTLPConfig.Endpoint,
					"--"+otlpmetrics.OtelMetricsExporterOTLPModeFlag, s.configuration.OTLPConfig.Metrics.OTLPConfig.Mode,
				)
				if s.configuration.OTLPConfig.Metrics.OTLPConfig.Insecure {
					args = append(args, "--"+otlpmetrics.OtelMetricsExporterOTLPInsecureFlag)
				}
			}
			if s.configuration.OTLPConfig.Metrics.RuntimeMetrics {
				args = append(args, "--"+otlpmetrics.OtelMetricsRuntimeFlag)
			}
			if s.configuration.OTLPConfig.Metrics.MinimumReadMemStatsInterval != 0 {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsRuntimeMinimumReadMemStatsIntervalFlag,
					s.configuration.OTLPConfig.Metrics.MinimumReadMemStatsInterval.String(),
				)
			}
			if s.configuration.OTLPConfig.Metrics.PushInterval != 0 {
				args = append(
					args,
					"--"+otlpmetrics.OtelMetricsExporterPushIntervalFlag,
					s.configuration.OTLPConfig.Metrics.PushInterval.String(),
				)
			}
			if len(s.configuration.OTLPConfig.Metrics.ResourceAttributes) > 0 {
				args = append(
					args,
					"--"+otlp.OtelResourceAttributesFlag,
					strings.Join(s.configuration.OTLPConfig.Metrics.ResourceAttributes, ","),
				)
			}
		}
		if s.configuration.OTLPConfig.BaseConfig.ServiceName != "" {
			args = append(args, "--"+otlp.OtelServiceNameFlag, s.configuration.OTLPConfig.BaseConfig.ServiceName)
		}
	}

	if s.configuration.Debug {
		args = append(args, "--"+service.DebugFlag)
	}

	s.t.Logf("Starting application with flags: %s", strings.Join(args, " "))
	rootCmd.SetArgs(args)
	rootCmd.SilenceErrors = true
	output := s.configuration.Output
	if output == nil {
		output = io.Discard
	}
	rootCmd.SetOut(output)
	rootCmd.SetErr(output)

	s.ctx = logging.TestingContext()
	s.ctx, s.cancel = context.WithCancel(s.ctx)
	s.ctx = service.ContextWithLifecycle(s.ctx)
	s.ctx = httpserver.ContextWithServerInfo(s.ctx)

	s.errorChan = make(chan error, 1)
	go func() {
		s.errorChan <- rootCmd.ExecuteContext(s.ctx)
	}()

	select {
	case <-service.Ready(s.ctx):
	case err := <-s.errorChan:
		if err != nil {
			require.NoError(s.t, err)
		} else {
			require.Fail(s.t, "unexpected service stop")
		}
	}

	var transport http.RoundTripper = &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
	}
	if s.configuration.Debug {
		transport = httpclient.NewDebugHTTPTransport(transport)
	}

	s.httpClient = ledgerclient.New(
		ledgerclient.WithServerURL(httpserver.URL(s.ctx)),
		ledgerclient.WithClient(&http.Client{
			Transport: transport,
		}),
	)
}

func (s *Server) Stop(ctx context.Context) {
	s.t.Helper()

	if s.cancel == nil {
		return
	}
	s.cancel()
	s.cancel = nil

	// Wait app to be marked as stopped
	select {
	case <-service.Stopped(s.ctx):
	case <-ctx.Done():
		require.Fail(s.t, "service should have been stopped")
	}

	// Ensure the app has been properly shutdown
	select {
	case err := <-s.errorChan:
		require.NoError(s.t, err)
	case <-ctx.Done():
		require.Fail(s.t, "service should have been stopped without error")
	}
}

func (s *Server) Client() *ledgerclient.Formance {
	return s.httpClient
}

func (s *Server) Restart(ctx context.Context) {
	s.t.Helper()

	s.Stop(ctx)
	s.Start()
}

func (s *Server) Database() *bun.DB {
	db, err := bunconnect.OpenSQLDB(s.ctx, s.configuration.PostgresConfiguration)
	require.NoError(s.t, err)
	s.t.Cleanup(func() {
		require.NoError(s.t, db.Close())
	})

	return db
}

func (s *Server) Subscribe() chan *nats.Msg {
	if s.configuration.NatsURL == "" {
		require.Fail(s.t, "NATS URL must be set")
	}

	ret := make(chan *nats.Msg)
	conn, err := nats.Connect(s.configuration.NatsURL)
	require.NoError(s.t, err)

	subscription, err := conn.Subscribe(s.id, func(msg *nats.Msg) {
		ret <- msg
	})
	require.NoError(s.t, err)
	s.t.Cleanup(func() {
		require.NoError(s.t, subscription.Unsubscribe())
	})
	return ret
}

func (s *Server) URL() string {
	return httpserver.URL(s.ctx)
}

func New(t T, configuration Configuration) *Server {
	t.Helper()

	srv := &Server{
		t:             t,
		configuration: configuration,
		id:            uuid.NewString()[:8],
	}
	t.Logf("Start testing server")
	srv.Start()
	t.Cleanup(func() {
		t.Logf("Stop testing server")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		srv.Stop(ctx)
	})

	return srv
}
