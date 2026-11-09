package controller

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestReconcileOperatorCredentialsCreatesScopedCredential(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}

	ready, err := r.reconcileOperatorCredentials(context.Background(), cluster)
	require.NoError(t, err)
	require.False(t, ready)
	require.Equal(t, operatorCredentialsSelectorValue(cluster), cluster.Labels[labelOperatorCredential])

	credentials := &ledgerv1alpha1.Credentials{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: operatorCredentialsName(cluster)}, credentials))
	require.Equal(t, operatorCredentialScopes, credentials.Spec.Scopes)
	require.False(t, credentials.Spec.Superuser)
	require.Equal(t, map[string]string{labelOperatorCredential: operatorCredentialsSelectorValue(cluster)}, credentials.Spec.Selector.MatchLabels)
}

func TestResolveMatchedClustersRestrictsOperatorCredentialsToOwner(t *testing.T) {
	scheme := authKeysScheme(t)
	owner := authEnabledCluster("ledger", "tenant-a", map[string]string{"shared": "yes"})
	other := authEnabledCluster("other", "tenant-b", map[string]string{"shared": "yes"})
	credentials := matchingCredentials("operator", map[string]string{"shared": "yes"}, false, "", "")
	credentials.Labels = map[string]string{labelManagedBy: "ledger-operator"}
	credentials.Annotations = map[string]string{annotationOperatorCluster: "tenant-a/ledger"}
	r := &CredentialsReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner, other).Build()}

	matched, err := r.resolveMatchedClusters(context.Background(), credentials)
	require.NoError(t, err)
	require.Equal(t, []ledgerv1alpha1.MatchedCluster{{Namespace: "tenant-a", Name: "ledger"}}, matched)

	delete(credentials.Annotations, annotationOperatorCluster)
	_, err = r.resolveMatchedClusters(context.Background(), credentials)
	require.ErrorContains(t, err, "has no Cluster owner annotation")

	delete(credentials.Labels, labelManagedBy)
	matched, err = r.resolveMatchedClusters(context.Background(), credentials)
	require.NoError(t, err)
	require.Len(t, matched, 2)
}

func TestValidateCredentialsAuthCompatibilityRejectsExplicitDisable(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", map[string]string{"tier": "gold"})
	*cluster.Spec.Auth.Enabled = false
	credentials := matchingCredentials("user-credentials", map[string]string{"tier": "gold"}, false, "", "")
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, credentials).Build(),
		Scheme: scheme,
	}

	err := r.validateCredentialsAuthCompatibility(context.Background(), cluster)
	require.ErrorContains(t, err, `auth.enabled=false conflicts with Credentials "user-credentials"`)
}

func TestValidateCredentialsAuthCompatibilityAllowsOperatorCredentialCleanup(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", map[string]string{"tier": "gold"})
	*cluster.Spec.Auth.Enabled = false
	credentials := matchingCredentials(operatorCredentialsName(cluster), map[string]string{"tier": "gold"}, false, "", "")
	credentials.Labels = map[string]string{labelOperatorCredential: operatorCredentialsSelectorValue(cluster), labelManagedBy: "ledger-operator"}
	credentials.Annotations = map[string]string{annotationOperatorCluster: cluster.Namespace + "/" + cluster.Name}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, credentials).Build(),
		Scheme: scheme,
	}

	require.NoError(t, r.validateCredentialsAuthCompatibility(context.Background(), cluster))
}

func TestValidateCredentialsAuthCompatibilityReturnsRetryableListError(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	*cluster.Spec.Auth.Enabled = false
	expected := errors.New("apiserver unavailable")
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{
		Client: interceptor.NewClient(base, interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*ledgerv1alpha1.CredentialsList); ok {
					return expected
				}

				return c.List(ctx, list, opts...)
			},
		}),
		Scheme: scheme,
	}

	err := r.validateCredentialsAuthCompatibility(context.Background(), cluster)
	require.ErrorIs(t, err, expected)
	require.ErrorIs(t, err, errCredentialsAuthValidationUnavailable)
}

func TestReconcileOperatorCredentialsDeletesWhenAuthDisabled(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{
		Name:        operatorCredentialsName(cluster),
		Labels:      map[string]string{labelManagedBy: "ledger-operator"},
		Annotations: map[string]string{annotationOperatorCluster: cluster.Namespace + "/" + cluster.Name},
	}}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, credentials).Build(),
		Scheme: scheme,
	}

	*cluster.Spec.Auth.Enabled = false
	ready, err := r.reconcileOperatorCredentials(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, ready)
	require.Error(t, r.Get(context.Background(), types.NamespacedName{Name: operatorCredentialsName(cluster)}, &ledgerv1alpha1.Credentials{}))
}

func TestReconcileOperatorCredentialsRejectsForeignCredential(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			cluster := authEnabledCluster("ledger", "tenant-a", nil)
			*cluster.Spec.Auth.Enabled = enabled
			foreign := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: operatorCredentialsName(cluster)}}
			foreign.Spec.Scopes = []string{"ledger:ReadLedger"}
			r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(authKeysScheme(t)).WithObjects(cluster, foreign).Build()}

			_, err := r.reconcileOperatorCredentials(context.Background(), cluster)
			if enabled {
				require.ErrorContains(t, err, "not owned by Cluster")
			} else {
				require.NoError(t, err)
			}
			actual := &ledgerv1alpha1.Credentials{}
			require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: foreign.Name}, actual))
			require.Equal(t, foreign.Spec.Scopes, actual.Spec.Scopes)
			require.Empty(t, actual.Annotations)
		})
	}
}
