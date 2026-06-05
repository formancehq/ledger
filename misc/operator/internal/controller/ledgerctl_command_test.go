package controller

import (
	"context"
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

			require.True(t, strings.HasPrefix(shell, "./ledgerctl store backup"),
				"shell command must start with the ledgerctl subcommand, got %q", shell)
			for _, fragment := range tt.wantContains {
				require.Contains(t, shell, fragment)
			}
			for _, fragment := range tt.wantNotContain {
				require.NotContains(t, shell, fragment)
			}
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
