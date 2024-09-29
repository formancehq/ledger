package testserver

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/publish"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/uptrace/bun"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/httpclient"
	"github.com/formancehq/go-libs/httpserver"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/service"
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

type Configuration struct {
	PostgresConfiguration bunconnect.ConnectionOptions
	NatsURL               string
	Output                io.Writer
	Debug                 bool
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
		"--" + bunconnect.PostgresURIFlag, s.configuration.PostgresConfiguration.DatabaseSourceName,
		"--" + bunconnect.PostgresMaxOpenConnsFlag, fmt.Sprint(s.configuration.PostgresConfiguration.MaxOpenConns),
		"--" + bunconnect.PostgresConnMaxIdleTimeFlag, fmt.Sprint(s.configuration.PostgresConfiguration.ConnMaxIdleTime),
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
	if testing.Verbose() {
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

func New(t T, configuration Configuration) *Server {
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
