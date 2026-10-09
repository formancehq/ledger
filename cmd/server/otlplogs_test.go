package server

import (
	"maps"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"

	otlp "github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

// newCmdWithLogFlags returns a fresh cobra.Command with the --debug and
// --log-level flags registered, mirroring the binary's flag set.
func newCmdWithLogFlags(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.Flags().Bool(service.DebugFlag, false, "Debug mode")
	cmd.Flags().String(LogLevelFlag, "", "Log level")

	return cmd
}

func TestResolveLogLevel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		logLevel  string
		debug     bool
		wantLevel logging.Level
		wantErr   bool
	}{
		{
			name:      "default is info",
			wantLevel: logging.InfoLevel,
		},
		{
			name:      "--debug=true maps to debug",
			debug:     true,
			wantLevel: logging.DebugLevel,
		},
		{
			name:      "--log-level=trace wins over default",
			logLevel:  "trace",
			wantLevel: logging.TraceLevel,
		},
		{
			name:      "--log-level=debug",
			logLevel:  "debug",
			wantLevel: logging.DebugLevel,
		},
		{
			name:      "--log-level=info",
			logLevel:  "info",
			wantLevel: logging.InfoLevel,
		},
		{
			name:      "--log-level=error",
			logLevel:  "error",
			wantLevel: logging.ErrorLevel,
		},
		{
			name:      "--log-level wins over --debug",
			logLevel:  "info",
			debug:     true,
			wantLevel: logging.InfoLevel,
		},
		{
			name:     "invalid --log-level errors",
			logLevel: "verbose",
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := newCmdWithLogFlags(t)
			if tc.debug {
				require.NoError(t, cmd.Flags().Set(service.DebugFlag, "true"))
			}
			if tc.logLevel != "" {
				require.NoError(t, cmd.Flags().Set(LogLevelFlag, tc.logLevel))
			}

			got, err := resolveLogLevel(cmd)
			if tc.wantErr {
				assert.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantLevel, got)
		})
	}
}

func TestResourceFromFlags(t *testing.T) {
	t.Parallel()
	defaultIdentity := map[string]string{
		"service.instance.id":          "cluster-a.42",
		"formance.ledger.cluster.id":   "cluster-a",
		"formance.ledger.cluster.name": "cluster-a",
		"formance.ledger.node.id":      "42",
	}
	withEnvironment := map[string]string{"deployment.environment": "regression"}
	maps.Copy(withEnvironment, defaultIdentity)
	for _, test := range []struct {
		name           string
		serviceName    string
		attributes     string
		wantName       string
		wantVersion    string
		wantAttributes map[string]string
	}{
		{name: "defaults and build metadata", wantName: "ledger", wantVersion: "v3.0.0+abc123", wantAttributes: defaultIdentity},
		{name: "explicit service", serviceName: "ledger-production", wantName: "ledger-production", wantVersion: "v3.0.0+abc123", wantAttributes: defaultIdentity},
		{name: "resource overrides", serviceName: "ledger-production", attributes: "service.name=override,service.version=override-build,deployment.environment=regression", wantName: "override", wantVersion: "override-build", wantAttributes: withEnvironment},
		{
			// An explicit cluster name without an explicit instance ID: the
			// default instance ID follows the name; the declared ID is kept.
			name: "explicit cluster name", attributes: "formance.ledger.cluster.name=prod-eu",
			wantName: "ledger", wantVersion: "v3.0.0+abc123",
			wantAttributes: map[string]string{
				"service.instance.id":          "prod-eu.42",
				"formance.ledger.cluster.id":   "cluster-a",
				"formance.ledger.cluster.name": "prod-eu",
				"formance.ledger.node.id":      "42",
			},
		},
		{
			// The operator's path: OTEL_RESOURCE_ATTRIBUTES is bound to the
			// flag, and the operator supplies a per-pod instance ID.
			name: "operator attributes", attributes: "env=prod,formance.ledger.cluster.name=prod-eu,k8s.namespace.name=payments,k8s.pod.name=ledger-prod-eu-1,k8s.container.name=ledger,service.instance.id=payments.ledger-prod-eu-1.ledger",
			wantName: "ledger", wantVersion: "v3.0.0+abc123",
			wantAttributes: map[string]string{
				"service.instance.id":          "payments.ledger-prod-eu-1.ledger",
				"formance.ledger.cluster.id":   "cluster-a",
				"formance.ledger.cluster.name": "prod-eu",
				"formance.ledger.node.id":      "42",
				"k8s.namespace.name":           "payments",
				"k8s.pod.name":                 "ledger-prod-eu-1",
			},
		},
		{
			name: "identity overrides", attributes: "service.instance.id=pod-0,formance.ledger.cluster.id=renamed,formance.ledger.node.id=7",
			wantName: "ledger", wantVersion: "v3.0.0+abc123",
			wantAttributes: map[string]string{
				"service.instance.id":        "pod-0",
				"formance.ledger.cluster.id": "renamed",
				"formance.ledger.node.id":    "7",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cmd := &cobra.Command{}
			otlp.AddFlags(cmd.Flags())
			require.NoError(t, cmd.Flags().Set(otlp.OtelServiceNameFlag, test.serviceName))
			if test.attributes != "" {
				require.NoError(t, cmd.Flags().Set(otlp.OtelResourceAttributesFlag, test.attributes))
			}
			res, err := resourceFromFlags(cmd, "cluster-a", 42, version.Info{Version: "v3.0.0", Commit: "abc123"})
			require.NoError(t, err)
			attributes := res.Set()
			name, ok := attributes.Value("service.name")
			require.True(t, ok)
			require.Equal(t, test.wantName, name.AsString())
			build, ok := attributes.Value("service.version")
			require.True(t, ok)
			require.Equal(t, test.wantVersion, build.AsString())
			for key, want := range test.wantAttributes {
				got, ok := attributes.Value(attribute.Key(key))
				require.True(t, ok, key)
				require.Equal(t, want, got.AsString(), key)
			}
		})
	}
}

func TestResourceFromFlagsRejectsMalformedAttributes(t *testing.T) {
	t.Parallel()
	cmd := &cobra.Command{}
	otlp.AddFlags(cmd.Flags())
	require.NoError(t, cmd.Flags().Set(otlp.OtelResourceAttributesFlag, "missing-value"))
	_, err := resourceFromFlags(cmd, "cluster-a", 42, version.Info{})
	require.ErrorContains(t, err, "malformed otlp attribute: missing-value")
}
