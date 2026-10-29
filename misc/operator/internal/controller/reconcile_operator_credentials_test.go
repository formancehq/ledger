package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func TestReconcileOperatorCredentialsDeletesWhenAuthDisabled(t *testing.T) {
	scheme := authKeysScheme(t)
	cluster := authEnabledCluster("ledger", "tenant-a", nil)
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: operatorCredentialsName(cluster)}}
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
