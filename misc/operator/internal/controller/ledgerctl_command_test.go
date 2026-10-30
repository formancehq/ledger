package controller

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
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

// TestLedgerctlCommand_TerminatorKeepsConnectionFlagsParsed runs the generated
// shell against a stub ledgerctl that echoes its argv. A ledger name such as
// "--help" is valid, so callers pass it after "--"; the connection flags must
// land before that terminator or ledgerctl would read them as positionals.
func TestLedgerctlCommand_TerminatorKeepsConnectionFlagsParsed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ledgerctl"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\n"), 0o755))

	cmd := ledgerctlCommand("addr:8888", tlsModeDisabled, "ledgers", "create", "--index", "reference", "--", "--help")
	run := exec.Command(cmd[0], cmd[1:]...)
	run.Dir = dir
	run.Env = []string{"CLUSTER_SECRET=token"}
	out, err := run.CombinedOutput()
	require.NoError(t, err, "command failed: %s", out)

	require.Equal(t, []string{
		"ledgers", "create", "--index", "reference",
		"--server", "addr:8888", "--insecure", "--auth-token", "token",
		"--", "--help",
	}, strings.Split(strings.TrimSuffix(string(out), "\n"), "\n"))
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
		{
			name:         "traces disabled at Cluster level disables the SDK despite a configured endpoint",
			env:          []string{"OTEL_TRACES=false", "OTEL_TRACES_EXPORTER_OTLP_ENDPOINT=collector:4317", "OTEL_TRACES_EXPORTER_OTLP_INSECURE=true"},
			wantDisabled: "true",
		},
		{
			name:         "traces enabled with an endpoint still translates",
			env:          []string{"OTEL_TRACES=true", "OTEL_TRACES_EXPORTER_OTLP_ENDPOINT=collector:4317", "OTEL_TRACES_EXPORTER_OTLP_INSECURE=true"},
			wantEndpoint: "http://collector:4317",
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

// tlsModeTestCluster is the Cluster the TLS-mode tests resolve: its
// StatefulSet is resourceName("example").
func tlsModeTestCluster() *ledgerv1alpha1.Cluster {
	return &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "ns", UID: "cluster-uid"}}
}

func TestFetchClusterStatefulSet(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))

	makeSTS := func(tlsMode string, controllerUID types.UID) *appsv1.StatefulSet {
		env := []corev1.EnvVar{{Name: "OTHER", Value: "x"}}
		if tlsMode != "" {
			env = append(env, corev1.EnvVar{Name: "TLS_MODE", Value: tlsMode})
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName("example"), Namespace: "ns"},
			Spec: appsv1.StatefulSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ledger", Env: env}},
					},
				},
			},
		}
		if controllerUID != "" {
			sts.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
				Name: "example", UID: controllerUID, Controller: new(true),
			}}
		}

		return sts
	}

	tests := []struct {
		name         string
		objects      []*appsv1.StatefulSet
		want         string
		wantAbsent   bool
		wantConflict bool
	}{
		{
			name:       "missing StatefulSet returns retryable lookup error",
			wantAbsent: true,
		},
		{
			name:    "reads TLS_MODE=required from running StatefulSet",
			objects: []*appsv1.StatefulSet{makeSTS(tlsModeRequired, "cluster-uid")},
			want:    tlsModeRequired,
		},
		{
			name:    "TLS_MODE unset returns empty",
			objects: []*appsv1.StatefulSet{makeSTS("", "cluster-uid")},
			want:    "",
		},
		{
			name:         "StatefulSet without controller reference is a name conflict",
			objects:      []*appsv1.StatefulSet{makeSTS(tlsModeRequired, "")},
			wantConflict: true,
		},
		{
			// A deleted Cluster recreated under the same name: the old
			// StatefulSet is still controlled by the old UID until GC.
			name:         "StatefulSet controlled by another Cluster UID is a name conflict",
			objects:      []*appsv1.StatefulSet{makeSTS(tlsModeRequired, "old-cluster-uid")},
			wantConflict: true,
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

			sts, err := fetchClusterStatefulSet(context.Background(), c, tlsModeTestCluster())
			switch {
			case tt.wantAbsent:
				require.ErrorIs(t, err, errClusterRuntimeAbsent)
				require.True(t, apierrors.IsNotFound(err), "got %v", err)
			case tt.wantConflict:
				require.ErrorIs(t, err, errResourceNameConflict)
				require.Nil(t, sts)
			default:
				require.NoError(t, err)
				require.Equal(t, tt.want, currentTLSModeFromStatefulSet(sts))
			}
		})
	}
}

// A recreated StatefulSet can temporarily be absent while its TLS pods survive.
// Neither exec caller may interpret that absence as a plaintext workload.
func TestLedgerctlExecOutputMissingTLSWorkload(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	for _, caller := range []string{"ledger", "cluster"} {
		t.Run(caller, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			var output string
			var err error
			// No exec configuration is provided: lookup must fail before reaching exec.
			if caller == "ledger" {
				r := &LedgerReconciler{Client: c}
				output, err = r.ledgerctlExecOutput(context.Background(), tlsModeTestCluster(), 8888, "status")
			} else {
				r := &ClusterReconciler{Client: c}
				output, err = r.ledgerctlExecOutput(context.Background(), tlsModeTestCluster(), 8888, "status")
			}
			require.ErrorContains(t, err, "resolving the StatefulSet of Cluster")
			require.ErrorIs(t, err, errClusterRuntimeAbsent)
			require.True(t, apierrors.IsNotFound(err), "got %v", err)
			require.Empty(t, output)
		})
	}
}

func TestFetchTLSModeRecoversAfterWorkloadRecreation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{tlsModeOptional, tlsModeRequired, tlsModeDisabled} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			scheme := runtime.NewScheme()
			require.NoError(t, appsv1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			cluster := tlsModeTestCluster()
			got, err := fetchClusterStatefulSet(ctx, c, cluster)
			require.True(t, apierrors.IsNotFound(err), "got %v", err)
			require.Nil(t, got)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: resourceName("example"), Namespace: "ns",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
						Name: cluster.Name, UID: cluster.UID, Controller: new(true),
					}},
				},
				Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "ledger", Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: mode}}}},
				}}},
			}
			require.NoError(t, c.Create(ctx, sts))
			got, err = fetchClusterStatefulSet(ctx, c, cluster)
			require.NoError(t, err)
			require.Equal(t, mode, currentTLSModeFromStatefulSet(got))
		})
	}
}
