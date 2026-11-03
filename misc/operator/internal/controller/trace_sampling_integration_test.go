//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestTraceSamplingAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		policy, arg string
		valid       bool
	}{
		{"always_on", "", true}, {"always_off", "", true},
		{"parentbased_always_on", "", true}, {"parentbased_always_off", "", true},
		{"traceidratio", "0", true}, {"traceidratio", "1", true},
		{"parentbased_traceidratio", "0.25", true}, {"traceidratio", "1e-2", true},
		{"", "", true}, {"unknown", "", false}, {"", "0.5", false},
		{"traceidratio", "", false}, {"parentbased_traceidratio", "", false},
		{"always_on", "0.5", false}, {"traceidratio", "-0.1", false},
		{"traceidratio", "1.1", false}, {"traceidratio", "NaN", false},
		{"traceidratio", "+Inf", false}, {"traceidratio", "invalid", false},
	} {
		t.Run(tc.policy+"/"+tc.arg, func(t *testing.T) {
			t.Parallel()
			cluster := traceSamplingCluster(t, tc.policy, tc.arg)
			cluster.Namespace = createTestNamespace(t)
			err := k8sClient.Create(ctx, cluster)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.True(t, apierrors.IsInvalid(err), "invalid sampling configuration must fail API admission: %v", err)
				require.Contains(t, err.Error(), "sampler")
			}
		})
	}
}

func TestTraceSamplingReconcileConflictThenFix(t *testing.T) {
	t.Parallel()
	for _, envName := range []string{"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", "TRACE_SAMPLER", "TRACE_SAMPLER_ARG"} {
		t.Run(envName, func(t *testing.T) {
			t.Parallel()
			cluster := newCluster("sampling-conflict", createTestNamespace(t))
			cluster.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{Traces: &ledgerv1alpha1.TracesConfig{
				Sampler: "parentbased_traceidratio", SamplerArg: "0.25",
			}}
			cluster.Spec.ExtraEnv = []corev1.EnvVar{{Name: envName, ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "sampling"}, Key: "policy"},
			}}}
			require.NoError(t, k8sClient.Create(ctx, cluster))
			key := client.ObjectKeyFromObject(cluster)
			requireEventually(t, func() bool {
				if err := k8sClient.Get(ctx, key, cluster); err != nil {
					return false
				}
				condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
				return condition != nil && condition.Status == metav1.ConditionFalse
			})
			condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
			require.Equal(t, "ValidationFailed", condition.Reason)
			require.Contains(t, condition.Message, envName+" in extraEnv")
			require.Equal(t, "Error", cluster.Status.Phase)
			var statefulSet appsv1.StatefulSet
			workloadKey := client.ObjectKey{Namespace: cluster.Namespace, Name: "ledger-" + cluster.Name}
			require.True(t, apierrors.IsNotFound(k8sClient.Get(ctx, workloadKey, &statefulSet)), "invalid config must not provision a StatefulSet")

			cluster.Spec.ExtraEnv = nil
			require.NoError(t, k8sClient.Update(ctx, cluster))
			requireEventually(t, func() bool {
				return k8sClient.Get(ctx, workloadKey, &statefulSet) == nil
			}, "correcting the conflict must resume reconciliation")
			require.NotEmpty(t, statefulSet.Spec.Template.Spec.Containers)
			envs := statefulSet.Spec.Template.Spec.Containers[0].Env
			assertEnv(t, envs, "OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
			assertEnv(t, envs, "OTEL_TRACES_SAMPLER_ARG", "0.25")
		})
	}
}
