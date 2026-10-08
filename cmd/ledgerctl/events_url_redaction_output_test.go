package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

type eventsURLRedactionServer struct {
	ledgerpb.UnimplementedBucketServiceServer

	response      *ledgerpb.GetEventsSinksResponse
	applyRequests chan *ledgerpb.ApplyRequest
}

func (s *eventsURLRedactionServer) GetEventsSinks(context.Context, *ledgerpb.GetEventsSinksRequest) (*ledgerpb.GetEventsSinksResponse, error) {
	return s.response, nil
}

func (s *eventsURLRedactionServer) Apply(_ context.Context, request *ledgerpb.ApplyRequest) (*ledgerpb.ApplyResponse, error) {
	s.applyRequests <- request

	return &ledgerpb.ApplyResponse{}, nil
}

// Sequential because command execution redirects process-global stdout and
// pterm output while the local gRPC fixture serves multiple output modes.
func TestEventsCommandsRedactURLCredentialsInOutput(t *testing.T) {
	const (
		natsPassword       = "nats-output-password"
		natsToken          = "nats-output-token"
		natsNoSchemePass   = "nats-schemeless-password"
		natsNoSchemeToken  = "nats-schemeless-token"
		httpPassword       = "http-output-password"
		clickHousePassword = "clickhouse-output-prefix@clickhouse-output-secret"
	)

	fixture := &eventsURLRedactionServer{
		response: &ledgerpb.GetEventsSinksResponse{
			Sinks: []*ledgerpb.SinkConfig{
				{
					Name: "stream",
					Type: &ledgerpb.SinkConfig_Nats{Nats: &ledgerpb.NatsSinkConfig{
						Url: "nats://operator:" + natsPassword + "@one:4222,nats://" + natsToken + "@two:4222," +
							"operator:" + natsNoSchemePass + "@three:4222," + natsNoSchemeToken + "@four:4222",
						Topic: "events",
					}},
				},
				{
					Name: "webhook",
					Type: &ledgerpb.SinkConfig_Http{Http: &ledgerpb.HttpSinkConfig{
						Endpoint: "https://operator:" + httpPassword + "@hooks.example/events",
					}},
				},
				{
					Name: "analytics",
					Type: &ledgerpb.SinkConfig_Clickhouse{Clickhouse: &ledgerpb.ClickHouseSinkConfig{
						Dsn: "clickhouse://db.example:9000/ledger?password=" + clickHousePassword + "&secure=true",
					}},
				},
				{
					Name: "kafka-empty-secret",
					Type: &ledgerpb.SinkConfig_Kafka{Kafka: &ledgerpb.KafkaSinkConfig{
						Brokers:       []string{"kafka.example:9092"},
						Topic:         "events",
						SaslMechanism: "PLAIN",
					}},
				},
				{
					Name: "databricks-empty-token",
					Type: &ledgerpb.SinkConfig_Databricks{Databricks: &ledgerpb.DatabricksSinkConfig{
						ServerHostname: "databricks.example",
						Auth:           &ledgerpb.DatabricksSinkConfig_Token{},
					}},
				},
				{
					Name: "databricks-empty-client-secret",
					Type: &ledgerpb.SinkConfig_Databricks{Databricks: &ledgerpb.DatabricksSinkConfig{
						ServerHostname: "databricks.example",
						Auth: &ledgerpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &ledgerpb.DatabricksOAuthM2M{
							ClientId: "client-id",
						}},
					}},
				},
			},
		},
		applyRequests: make(chan *ledgerpb.ApplyRequest, 16),
	}
	listControls := []string{
		"stream", "operator", "events", "one:4222", "two:4222", "three:4222", "four:4222",
		"hooks.example", "db.example", "secure=true",
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	ledgerpb.RegisterBucketServiceServer(server, fixture)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveResult)
	})

	configDir := t.TempDir()
	t.Setenv("HOME", configDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("LEDGERCTL_PROFILE", "")

	for _, format := range []string{"table", "json", "yaml"} {
		t.Run("list "+format, func(t *testing.T) {
			args := []string{
				"events", "list", "--server", listener.Addr().String(), "--insecure",
				"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
				"--signing-key=", "--response-verify-key=", "--auth-token=test-token",
			}
			var resultPath string
			if format != "table" {
				args = append(args, "--"+format)
			}
			if format == "json" {
				resultPath = filepath.Join(t.TempDir(), "result.json")
				require.NoError(t, os.WriteFile(resultPath, nil, 0o600))
				args = append(args, "--result-file="+resultPath)
			}

			output := executeEventsCommand(t, args)
			for _, credential := range []string{natsPassword, natsToken, natsNoSchemePass, natsNoSchemeToken, httpPassword, "clickhouse-output-prefix", "clickhouse-output-secret"} {
				assert.NotContains(t, output, credential)
			}
			for _, control := range listControls {
				assert.Contains(t, output, control)
			}
			if format == "table" {
				assert.NotContains(t, output, "(set)")
				assert.Equal(t, 4, strings.Count(output, "(none)"))
			}

			if resultPath != "" {
				result, err := os.ReadFile(resultPath)
				require.NoError(t, err)
				for _, credential := range []string{natsPassword, natsToken, natsNoSchemePass, natsNoSchemeToken, httpPassword, "clickhouse-output-prefix", "clickhouse-output-secret"} {
					assert.NotContains(t, string(result), credential)
				}
				for _, control := range listControls {
					assert.Contains(t, string(result), control)
				}
			}
		})
	}

	addCases := []struct {
		name        string
		flags       []string
		credentials []string
		controls    []string
	}{
		{
			name: "NATS password",
			flags: []string{
				"--nats-url", "nats://operator:" + natsPassword + "@one:4222",
				"--nats-topic", "events",
			},
			credentials: []string{natsPassword},
			controls:    []string{"stream", "operator", "one:4222", "events"},
		},
		{
			name: "NATS token",
			flags: []string{
				"--nats-url", "nats://" + natsToken + "@two:4222",
				"--nats-topic", "events",
			},
			credentials: []string{natsToken},
			controls:    []string{"stream", "two:4222", "events"},
		},
		{
			name: "scheme-less NATS password",
			flags: []string{
				"--nats-url", "operator:" + natsNoSchemePass + "@three:4222",
				"--nats-topic", "events",
			},
			credentials: []string{natsNoSchemePass},
			controls:    []string{"stream", "operator", "three:4222", "events"},
		},
		{
			name: "scheme-less NATS token",
			flags: []string{
				"--nats-url", natsNoSchemeToken + "@four:4222",
				"--nats-topic", "events",
			},
			credentials: []string{natsNoSchemeToken},
			controls:    []string{"stream", "four:4222", "events"},
		},
		{
			name: "HTTP password",
			flags: []string{
				"--http-endpoint", "https://operator:" + httpPassword + "@hooks.example/events",
			},
			credentials: []string{httpPassword},
			controls:    []string{"stream", "operator", "hooks.example"},
		},
		{
			name: "ClickHouse query password",
			flags: []string{
				"--clickhouse-dsn", "clickhouse://db.example:9000/ledger?password=" + clickHousePassword + "&secure=true",
			},
			credentials: []string{"clickhouse-output-prefix", "clickhouse-output-secret"},
			controls:    []string{"stream", "db.example", "secure=true"},
		},
	}
	for _, format := range []string{"table", "json", "yaml"} {
		for _, addCase := range addCases {
			t.Run("add-sink "+format+" "+addCase.name, func(t *testing.T) {
				args := []string{
					"events", "add-sink", "--name", "stream",
					"--server", listener.Addr().String(), "--insecure",
					"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
					"--signing-key=", "--response-verify-key=", "--auth-token=test-token",
				}
				args = append(args, addCase.flags...)
				var resultPath string
				if format != "table" {
					args = append(args, "--"+format)
				}
				if format == "json" {
					resultPath = filepath.Join(t.TempDir(), "result.json")
					require.NoError(t, os.WriteFile(resultPath, nil, 0o600))
					args = append(args, "--result-file="+resultPath)
				}

				output := executeEventsCommand(t, args)
				for _, credential := range addCase.credentials {
					assert.NotContains(t, output, credential)
				}
				for _, control := range addCase.controls {
					assert.Contains(t, output, control)
				}
				if resultPath != "" {
					result, err := os.ReadFile(resultPath)
					require.NoError(t, err)
					for _, credential := range addCase.credentials {
						assert.NotContains(t, string(result), credential)
					}
					for _, control := range addCase.controls {
						assert.Contains(t, string(result), control)
					}
				}

				select {
				case request := <-fixture.applyRequests:
					for _, credential := range addCase.credentials {
						assert.Contains(t, request.String(), credential, "Apply must receive the original credential")
					}
				default:
					t.Fatal("events add-sink did not call Apply")
				}
			})
		}
	}
}

func executeEventsCommand(t *testing.T, args []string) string {
	t.Helper()

	stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
	require.NoError(t, err)
	originalStdout := os.Stdout
	os.Stdout = stdout
	pterm.SetDefaultOutput(stdout)
	defer func() {
		os.Stdout = originalStdout
		pterm.SetDefaultOutput(originalStdout)
	}()

	root := newRootCommand()
	root.SetArgs(args)
	require.NoError(t, root.ExecuteContext(t.Context()))
	require.NoError(t, stdout.Close())

	output, err := os.ReadFile(stdout.Name())
	require.NoError(t, err)

	return string(output)
}
