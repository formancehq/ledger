package controller

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func traceSamplingCluster(t *testing.T, sampler, arg string) *ledgerv1alpha1.Cluster {
	t.Helper()
	cluster := newMinimalCluster()
	data, err := json.Marshal(map[string]string{"sampler": sampler, "samplerArg": arg})
	require.NoError(t, err)
	var traces ledgerv1alpha1.TracesConfig
	require.NoError(t, json.Unmarshal(data, &traces))
	cluster.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{Traces: &traces}

	return cluster
}

func TestTraceSamplingValidationAndEnv(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"always_on", "always_off", "parentbased_always_on", "parentbased_always_off", "traceidratio", "parentbased_traceidratio"} {
		args := []string{""}
		if policy == "traceidratio" || policy == "parentbased_traceidratio" {
			args = []string{"0", "1", "0.25", "1e-2"}
		}
		for _, arg := range args {
			t.Run(policy+"/"+arg, func(t *testing.T) {
				t.Parallel()
				cluster := traceSamplingCluster(t, policy, arg)
				require.NoError(t, validateSpec(cluster))
				envs := buildEnvVars(cluster, "disabled", nil)
				assertEnv(t, envs, "OTEL_TRACES_SAMPLER", policy)
				if arg == "" {
					assertNoEnv(t, envs, "OTEL_TRACES_SAMPLER_ARG")
				} else {
					assertEnv(t, envs, "OTEL_TRACES_SAMPLER_ARG", arg)
				}
			})
		}
	}
}

func TestTraceSamplingRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ policy, arg, message string }{
		{"custom", "", "sampler"}, {"", "0.5", "samplerArg"},
		{"traceidratio", "", "samplerArg"}, {"parentbased_traceidratio", "", "samplerArg"},
		{"always_on", "0.5", "samplerArg"}, {"always_off", "0.5", "samplerArg"},
		{"parentbased_always_on", "0.5", "samplerArg"}, {"parentbased_always_off", "0.5", "samplerArg"},
		{"traceidratio", "-0.1", "samplerArg"}, {"traceidratio", "1.1", "samplerArg"},
		{"traceidratio", "NaN", "samplerArg"}, {"traceidratio", "+Inf", "samplerArg"},
		{"traceidratio", "-Inf", "samplerArg"}, {"traceidratio", "invalid", "samplerArg"},
	} {
		t.Run(tc.policy+"/"+tc.arg, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, validateSpec(traceSamplingCluster(t, tc.policy, tc.arg)), tc.message)
		})
	}
}

func TestTraceSamplingRawEnvCompatibility(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", "TRACE_SAMPLER", "TRACE_SAMPLER_ARG"} {
		for _, fromSecret := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/secret=%t", name, fromSecret), func(t *testing.T) {
				t.Parallel()
				raw := corev1.EnvVar{Name: name, Value: "raw-sdk-value"}
				if fromSecret {
					raw.Value = ""
					raw.ValueFrom = &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "sampling"}, Key: "value"}}
				}
				cluster := traceSamplingCluster(t, "always_on", "")
				cluster.Spec.ExtraEnv = []corev1.EnvVar{raw}
				require.ErrorContains(t, validateSpec(cluster), "extraEnv")
				cluster = traceSamplingCluster(t, "", "")
				cluster.Spec.ExtraEnv = []corev1.EnvVar{raw}
				require.NoError(t, validateSpec(cluster))
				envs := buildEnvVars(cluster, "disabled", nil)
				require.Equal(t, &raw, findEnv(envs, name))
			})
		}
	}
	for _, cluster := range []*ledgerv1alpha1.Cluster{newMinimalCluster(), traceSamplingCluster(t, "", "")} {
		require.NoError(t, validateSpec(cluster))
		envs := buildEnvVars(cluster, "disabled", nil)
		assertNoEnv(t, envs, "OTEL_TRACES_SAMPLER")
		assertNoEnv(t, envs, "OTEL_TRACES_SAMPLER_ARG")
	}
}
