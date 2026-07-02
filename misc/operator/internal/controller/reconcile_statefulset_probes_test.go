package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// TestBuildPodTemplate_NormalModeUsesHTTPProbes locks in the existing
// HTTP-probe defaults so a future change cannot silently downgrade the
// readiness signal in normal mode.
func TestBuildPodTemplate_NormalModeUsesHTTPProbes(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			HttpPort: 9000,
			GrpcPort: 8888,
		},
	}

	tpl := buildPodTemplate(ls, "hash", nil, "")
	require.Len(t, tpl.Spec.Containers, 1)
	c := tpl.Spec.Containers[0]

	require.NotNil(t, c.LivenessProbe)
	require.NotNil(t, c.LivenessProbe.HTTPGet)
	assert.Equal(t, "/livez", c.LivenessProbe.HTTPGet.Path)
	assert.Equal(t, intstr.FromString("http"), c.LivenessProbe.HTTPGet.Port)

	require.NotNil(t, c.ReadinessProbe)
	require.NotNil(t, c.ReadinessProbe.HTTPGet)
	assert.Equal(t, "/readyz", c.ReadinessProbe.HTTPGet.Path)

	require.NotNil(t, c.StartupProbe)
	require.NotNil(t, c.StartupProbe.HTTPGet)
	assert.Equal(t, "/livez", c.StartupProbe.HTTPGet.Path)
}

// TestBuildPodTemplate_RestoreModeUsesTCPProbes is the headline guarantee:
// restore mode does not expose /livez or /readyz on the management HTTP
// server, so HTTP probes would loop forever and leave the pod NotReady. The
// operator must fall back to TCP probes on the gRPC port so the restore RPC
// server can actually be reached through the Service / Ingress.
func TestBuildPodTemplate_RestoreModeUsesTCPProbes(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			HttpPort: 9000,
			GrpcPort: 8888,
			Restore:  true,
		},
	}

	tpl := buildPodTemplate(ls, "hash", nil, "")
	require.Len(t, tpl.Spec.Containers, 1)
	c := tpl.Spec.Containers[0]

	require.NotNil(t, c.LivenessProbe)
	assert.Nil(t, c.LivenessProbe.HTTPGet, "restore-mode liveness must not be HTTP")
	require.NotNil(t, c.LivenessProbe.TCPSocket)
	assert.Equal(t, intstr.FromString("grpc"), c.LivenessProbe.TCPSocket.Port)

	require.NotNil(t, c.ReadinessProbe)
	assert.Nil(t, c.ReadinessProbe.HTTPGet, "restore-mode readiness must not be HTTP")
	require.NotNil(t, c.ReadinessProbe.TCPSocket)
	assert.Equal(t, intstr.FromString("grpc"), c.ReadinessProbe.TCPSocket.Port)

	require.NotNil(t, c.StartupProbe)
	assert.Nil(t, c.StartupProbe.HTTPGet, "restore-mode startup must not be HTTP")
	require.NotNil(t, c.StartupProbe.TCPSocket)
	assert.Equal(t, intstr.FromString("grpc"), c.StartupProbe.TCPSocket.Port)
}

// TestBuildPodTemplate_RestoreModeHonorsUserProbeOverride verifies that the
// usual user-override mechanism (mergeProbe) still applies in restore mode —
// if a user provides a custom readinessProbe, it must win over the TCP
// fallback the same way it would win over the HTTP default in normal mode.
func TestBuildPodTemplate_RestoreModeHonorsUserProbeOverride(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			HttpPort: 9000,
			GrpcPort: 8888,
			Restore:  true,
			ReadinessProbe: &corev1.Probe{
				PeriodSeconds: 17,
			},
		},
	}

	tpl := buildPodTemplate(ls, "hash", nil, "")
	c := tpl.Spec.Containers[0]

	require.NotNil(t, c.ReadinessProbe)
	require.NotNil(t, c.ReadinessProbe.TCPSocket)
	assert.Equal(t, intstr.FromString("grpc"), c.ReadinessProbe.TCPSocket.Port)
	assert.Equal(t, int32(17), c.ReadinessProbe.PeriodSeconds)
}
