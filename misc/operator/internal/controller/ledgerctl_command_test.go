package controller

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLedgerctlTLSFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tlsMode string
		want    string
	}{
		{"disabled stays insecure", tlsModeDisabled, "--insecure"},
		{"empty (bootstrap) stays insecure", "", "--insecure"},
		{"unknown value stays insecure", "wat", "--insecure"},
		{"optional uses TLS CA", tlsModeOptional, `--tls-ca-cert "$TLS_CA_CERT_FILE"`},
		{"required uses TLS CA", tlsModeRequired, `--tls-ca-cert "$TLS_CA_CERT_FILE"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, ledgerctlTLSFlag(tt.tlsMode))
		})
	}
}

func TestLedgerctlCommand_TLSModes(t *testing.T) {
	t.Parallel()

	const serverAddr = "ledger.ledger-v3.svc.cluster.local:8888"

	tests := []struct {
		name           string
		tlsMode        string
		wantContains   []string
		wantNotContain []string
	}{
		{
			name:           "disabled emits --insecure",
			tlsMode:        tlsModeDisabled,
			wantContains:   []string{"--insecure", `--auth-token "$CLUSTER_SECRET"`, `--server "` + serverAddr + `"`},
			wantNotContain: []string{"--tls-ca-cert"},
		},
		{
			name:           "required emits --tls-ca-cert and no --insecure",
			tlsMode:        tlsModeRequired,
			wantContains:   []string{`--tls-ca-cert "$TLS_CA_CERT_FILE"`, `--auth-token "$CLUSTER_SECRET"`, `--server "` + serverAddr + `"`},
			wantNotContain: []string{"--insecure"},
		},
		{
			name:           "optional emits --tls-ca-cert and no --insecure",
			tlsMode:        tlsModeOptional,
			wantContains:   []string{`--tls-ca-cert "$TLS_CA_CERT_FILE"`, `--auth-token "$CLUSTER_SECRET"`},
			wantNotContain: []string{"--insecure"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := ledgerctlCommand(serverAddr, tt.tlsMode, "store", "backup")
			require.Equal(t, []string{"/bin/sh", "-c"}, cmd[:2], "command must be wrapped in sh -c")
			shell := cmd[2]

			// Args are single-quoted to neutralize shell metacharacters that
			// can reach this helper from CRD fields or Secret values. The
			// ledgerctl invocation is preceded by the OTEL env prologue.
			require.Contains(t, shell, "./ledgerctl 'store' 'backup'",
				"shell command must contain the (quoted) ledgerctl subcommand, got %q", shell)
			require.True(t, strings.HasPrefix(shell, otelExecPrologue),
				"shell command must start with the OTEL env prologue, got %q", shell)
			for _, fragment := range tt.wantContains {
				require.Contains(t, shell, fragment)
			}
			for _, fragment := range tt.wantNotContain {
				require.NotContains(t, shell, fragment)
			}
		})
	}
}

func TestOtelExecPrologue(t *testing.T) {
	t.Parallel()

	// Execute the prologue in a real shell and report the resolved values so
	// the test exercises the actual POSIX-sh logic the pod runs, not a Go
	// re-implementation of it.
	run := func(env []string) (endpoint, protocol, sdkDisabled string) {
		t.Helper()
		script := otelExecPrologue + `printf '%s\n%s\n%s\n' "${OTEL_EXPORTER_OTLP_ENDPOINT:-}" "${OTEL_EXPORTER_OTLP_PROTOCOL:-}" "${OTEL_SDK_DISABLED:-}"`
		cmd := exec.Command("/bin/sh", "-c", script)
		// Run with an explicit environment (never nil): a nil Env makes
		// exec.Command inherit the parent's, so an OTEL_* var on the developer's
		// or CI runner's machine would leak in and make the assertion flaky.
		cmd.Env = append([]string{}, env...)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "prologue failed: %s", out)
		// Three printf lines; do not trim, so an empty value keeps its line.
		lines := strings.Split(string(out), "\n")
		require.GreaterOrEqual(t, len(lines), 3, "unexpected output: %q", string(out))

		return lines[0], lines[1], lines[2]
	}

	tests := []struct {
		name         string
		env          []string
		wantEndpoint string
		wantProtocol string
		wantDisabled string
	}{
		{
			name:         "no endpoint disables the SDK",
			env:          nil,
			wantDisabled: "true",
		},
		{
			name:         "insecure endpoint gets http scheme",
			env:          []string{"OTEL_TRACES_EXPORTER_OTLP_ENDPOINT=collector.monitoring.svc:4317", "OTEL_TRACES_EXPORTER_OTLP_INSECURE=true"},
			wantEndpoint: "http://collector.monitoring.svc:4317",
		},
		{
			name:         "secure endpoint gets https scheme",
			env:          []string{"OTEL_TRACES_EXPORTER_OTLP_ENDPOINT=collector.monitoring.svc:4317"},
			wantEndpoint: "https://collector.monitoring.svc:4317",
		},
		{
			name:         "endpoint already carrying a scheme is left untouched",
			env:          []string{"OTEL_TRACES_EXPORTER_OTLP_ENDPOINT=http://collector:4317", "OTEL_TRACES_EXPORTER_OTLP_INSECURE=true"},
			wantEndpoint: "http://collector:4317",
		},
		{
			name:         "http mode is translated to the standard protocol",
			env:          []string{"OTEL_TRACES_EXPORTER_OTLP_ENDPOINT=collector:4318", "OTEL_TRACES_EXPORTER_OTLP_INSECURE=true", "OTEL_TRACES_EXPORTER_OTLP_MODE=http"},
			wantEndpoint: "http://collector:4318",
			wantProtocol: "http",
		},
		{
			name:         "grpc mode is translated to the standard protocol",
			env:          []string{"OTEL_TRACES_EXPORTER_OTLP_ENDPOINT=collector:4317", "OTEL_TRACES_EXPORTER_OTLP_INSECURE=true", "OTEL_TRACES_EXPORTER_OTLP_MODE=grpc"},
			wantEndpoint: "http://collector:4317",
			wantProtocol: "grpc",
		},
		{
			name:         "mode without an endpoint still disables the SDK",
			env:          []string{"OTEL_TRACES_EXPORTER_OTLP_MODE=http"},
			wantDisabled: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			endpoint, protocol, disabled := run(tt.env)
			require.Equal(t, tt.wantEndpoint, endpoint)
			require.Equal(t, tt.wantProtocol, protocol)
			require.Equal(t, tt.wantDisabled, disabled)
		})
	}
}

func TestPodSelfServerAddr(t *testing.T) {
	t.Parallel()

	got := podSelfServerAddr("ledger-headless", 8888)
	require.Equal(t, "$POD_NAME.ledger-headless.$POD_NAMESPACE.svc.cluster.local:8888", got)
}

func TestFetchTLSMode(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))

	makeSTS := func(name, namespace, tlsMode string) *appsv1.StatefulSet {
		env := []corev1.EnvVar{{Name: "OTHER", Value: "x"}}
		if tlsMode != "" {
			env = append(env, corev1.EnvVar{Name: "TLS_MODE", Value: tlsMode})
		}

		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1.StatefulSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ledger", Env: env}},
					},
				},
			},
		}
	}

	tests := []struct {
		name    string
		objects []*appsv1.StatefulSet
		nsName  string
		stsName string
		want    string
		wantErr bool
	}{
		{
			name:    "missing StatefulSet returns empty without error",
			nsName:  "ns",
			stsName: "ledger",
			want:    "",
		},
		{
			name:    "reads TLS_MODE=required from running StatefulSet",
			objects: []*appsv1.StatefulSet{makeSTS("ledger", "ns", tlsModeRequired)},
			nsName:  "ns",
			stsName: "ledger",
			want:    tlsModeRequired,
		},
		{
			name:    "TLS_MODE unset returns empty",
			objects: []*appsv1.StatefulSet{makeSTS("ledger", "ns", "")},
			nsName:  "ns",
			stsName: "ledger",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			builder := fake.NewClientBuilder().WithScheme(scheme)
			for _, sts := range tt.objects {
				builder = builder.WithObjects(sts)
			}
			c := builder.Build()

			got, err := fetchTLSMode(context.Background(), c, tt.nsName, tt.stsName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
