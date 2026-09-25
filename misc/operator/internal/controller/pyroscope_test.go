package controller

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestPyroscopeSecretReferences(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                     string
		enabled, token, password bool
	}{
		{name: "disabled with credentials", token: true, password: true},
		{name: "enabled without credentials", enabled: true},
		{name: "token", enabled: true, token: true},
		{name: "basic auth", enabled: true, password: true},
		{name: "both", enabled: true, token: true, password: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "profiling", Namespace: "default"},
				StringData: map[string]string{
					"token":    "AUDIT_PYRO_TOKEN_9f261",
					"password": "AUDIT_PYRO_PASSWORD_7a243",
				},
			}
			ls := newMinimalCluster()
			pyro := &ledgerv1alpha1.PyroscopeConfig{Enabled: tc.enabled}
			ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{ServiceName: "ledger-profile", Pyroscope: pyro}
			if tc.token {
				pyro.AuthTokenFrom = &ledgerv1alpha1.SecretKeyRef{Name: secret.Name, Key: "token"}
			}
			if tc.password {
				pyro.BasicAuthPasswordFrom = &ledgerv1alpha1.SecretKeyRef{Name: secret.Name, Key: "password"}
			}
			before := ls.DeepCopy()
			sts := &appsv1.StatefulSet{Spec: buildStatefulSetSpec(ls, computeSpecHash(&ls.Spec), nil, "disabled")}
			require.Equal(t, before, ls, "rendering must not mutate the Cluster")
			envs := sts.Spec.Template.Spec.Containers[0].Env
			if tc.enabled {
				assertEnv(t, envs, "PYROSCOPE_ENABLED", "true")
				assertEnv(t, envs, "PYROSCOPE_APPLICATION_NAME", "ledger-profile")
			} else {
				for _, env := range envs {
					require.NotContains(t, env.Name, "PYROSCOPE_")
				}
			}
			for name, key := range map[string]string{"PYROSCOPE_AUTH_TOKEN": "token", "PYROSCOPE_BASIC_AUTH_PASSWORD": "password"} {
				configured := tc.token
				if key == "password" {
					configured = tc.password
				}
				if !tc.enabled || !configured {
					assertNoEnv(t, envs, name)

					continue
				}
				env := findEnv(envs, name)
				require.NotNil(t, env)
				require.Empty(t, env.Value)
				require.NotNil(t, env.ValueFrom)
				require.Equal(t, &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}, Key: key,
				}, env.ValueFrom.SecretKeyRef)
				require.NotEmpty(t, secret.StringData[env.ValueFrom.SecretKeyRef.Key], "reference must select the intended credential")
			}
			for _, obj := range []any{ls, sts, &sts.Spec.Template} {
				for _, marshal := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
					data, err := marshal(obj)
					require.NoError(t, err)
					for _, canary := range secret.StringData {
						require.NotContains(t, string(data), canary)
					}
				}
			}
		})
	}
}

func TestPyroscopeProfilingOptions(t *testing.T) {
	t.Parallel()
	ls := newMinimalCluster()
	mutex, block, disableGC := int32(0), int32(7), false
	ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{
		ServiceName: "fallback",
		Pyroscope: &ledgerv1alpha1.PyroscopeConfig{
			Enabled: true, ServerAddress: "https://profiles.example.com", ApplicationName: "custom",
			TenantID: "tenant", BasicAuthUser: "user", UploadRate: "30s", Tags: "env=test",
			ProfileTypes: "cpu,goroutines", MutexProfileFraction: &mutex, BlockProfileRate: &block, DisableGCRuns: &disableGC,
		},
	}
	envs := buildEnvVars(ls, "disabled", nil)
	for name, value := range map[string]string{
		"PYROSCOPE_ENABLED": "true", "PYROSCOPE_SERVER_ADDRESS": "https://profiles.example.com",
		"PYROSCOPE_APPLICATION_NAME": "custom", "PYROSCOPE_TENANT_ID": "tenant", "PYROSCOPE_BASIC_AUTH_USER": "user",
		"PYROSCOPE_UPLOAD_RATE": "30s", "PYROSCOPE_TAGS": "env=test", "PYROSCOPE_PROFILE_TYPES": "cpu,goroutines",
		"PYROSCOPE_MUTEX_PROFILE_FRACTION": "0", "PYROSCOPE_BLOCK_PROFILE_RATE": "7", "PYROSCOPE_DISABLE_GC_RUNS": "false",
	} {
		assertEnv(t, envs, name, value)
	}
}

func TestPyroscopeReferenceUpdate(t *testing.T) {
	t.Parallel()
	ls := newMinimalCluster()
	ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{Pyroscope: &ledgerv1alpha1.PyroscopeConfig{
		Enabled:               true,
		AuthTokenFrom:         &ledgerv1alpha1.SecretKeyRef{Name: "profiling", Key: "token"},
		BasicAuthPasswordFrom: &ledgerv1alpha1.SecretKeyRef{Name: "profiling", Key: "password"},
	}}
	updated := ls.DeepCopy()
	updated.Spec.Monitoring.Pyroscope.AuthTokenFrom.Name = "new-profiling"
	updated.Spec.Monitoring.Pyroscope.BasicAuthPasswordFrom.Key = "new-password"
	require.Equal(t, "profiling", ls.Spec.Monitoring.Pyroscope.AuthTokenFrom.Name)
	require.Equal(t, "password", ls.Spec.Monitoring.Pyroscope.BasicAuthPasswordFrom.Key)
	require.NotEqual(t, computeSpecHash(&ls.Spec), computeSpecHash(&updated.Spec), "reference changes must trigger a rollout")
	envs := buildStatefulSetSpec(updated, computeSpecHash(&updated.Spec), nil, "disabled").Template.Spec.Containers[0].Env
	require.Equal(t, "new-profiling", findEnv(envs, "PYROSCOPE_AUTH_TOKEN").ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "new-password", findEnv(envs, "PYROSCOPE_BASIC_AUTH_PASSWORD").ValueFrom.SecretKeyRef.Key)
}
