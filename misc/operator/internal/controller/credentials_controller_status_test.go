package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestCredentialsReconcileFailureStatus(t *testing.T) {
	t.Parallel()

	for _, failure := range []struct {
		name          string
		conditionType string
		reason        string
		errorContext  string
	}{
		{name: "selector", conditionType: "SelectorResolved", reason: "SelectorFailed", errorContext: "resolving matched clusters"},
		{name: "canonical", conditionType: "Ready", reason: "CanonicalFailed", errorContext: "ensuring canonical secret"},
		{name: "replica", conditionType: "Ready", reason: "SecretFailed", errorContext: `ensuring secret in "target"`},
	} {
		for _, statusFails := range []bool{true, false} {
			statusName := "status succeeds"
			if statusFails {
				statusName = "status fails"
			}
			t.Run(failure.name+"/"+statusName, func(t *testing.T) {
				t.Parallel()

				scheme := runtime.NewScheme()
				require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
				require.NoError(t, corev1.AddToScheme(scheme))
				credentials := &ledgerv1alpha1.Credentials{
					ObjectMeta: metav1.ObjectMeta{
						Name: "credentials", UID: "credentials-uid", Generation: 7,
						Finalizers: []string{credentialsFinalizer},
					},
					Spec: ledgerv1alpha1.CredentialsSpec{AdditionalNamespaces: []string{"target"}},
				}
				canonical := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name: credentialsCanonicalSecretName(credentials.Name), Namespace: "operator",
						Labels:          map[string]string{credentialsNameLabel: credentials.Name},
						OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(credentials, ledgerv1alpha1.GroupVersion.WithKind("Credentials"))},
					},
					Data: map[string][]byte{keySeedHex: []byte("test-seed")},
				}
				baseClient := fake.NewClientBuilder().WithScheme(scheme).
					WithStatusSubresource(&ledgerv1alpha1.Credentials{}).
					WithObjects(credentials, canonical).Build()
				reconcileErr := errors.New("reconciliation failed")
				statusErr := errors.New("status update failed")
				failureCalls := 0
				statusCalls := 0
				var attemptedStatus *ledgerv1alpha1.Credentials
				wrappedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
					List: func(ctx context.Context, current client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*ledgerv1alpha1.ClusterList); ok && failure.name == "selector" {
							failureCalls++

							return reconcileErr
						}

						return current.List(ctx, list, opts...)
					},
					Get: func(ctx context.Context, current client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*corev1.Secret); ok && key.Namespace == "operator" && failure.name == "canonical" {
							failureCalls++

							return reconcileErr
						}

						return current.Get(ctx, key, obj, opts...)
					},
					Create: func(ctx context.Context, current client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if _, ok := obj.(*corev1.Secret); ok && obj.GetNamespace() == "target" && failure.name == "replica" {
							failureCalls++

							return reconcileErr
						}

						return current.Create(ctx, obj, opts...)
					},
					SubResourceUpdate: func(ctx context.Context, current client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						require.Equal(t, "status", subResource)
						statusCalls++
						attemptedStatus = obj.(*ledgerv1alpha1.Credentials).DeepCopy()
						if statusFails {
							return statusErr
						}

						return current.SubResource(subResource).Update(ctx, obj, opts...)
					},
				})
				reconciler := &CredentialsReconciler{
					Client: wrappedClient, APIReader: wrappedClient, Scheme: scheme, OperatorNamespace: "operator",
				}

				result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
					NamespacedName: types.NamespacedName{Name: credentials.Name},
				})

				require.Equal(t, ctrl.Result{}, result)
				require.ErrorIs(t, err, reconcileErr)
				require.ErrorContains(t, err, failure.errorContext)
				if statusFails {
					require.ErrorIs(t, err, statusErr)
					require.ErrorContains(t, err, "updating credentials error status")
				} else {
					require.NotErrorIs(t, err, statusErr)
				}
				require.Equal(t, 1, failureCalls)
				require.Equal(t, 1, statusCalls)
				require.NotNil(t, attemptedStatus)
				require.Equal(t, "Error", attemptedStatus.Status.Phase)
				condition := meta.FindStatusCondition(attemptedStatus.Status.Conditions, failure.conditionType)
				require.NotNil(t, condition)
				require.Equal(t, metav1.ConditionFalse, condition.Status)
				require.Equal(t, failure.reason, condition.Reason)
				require.Equal(t, credentials.Generation, condition.ObservedGeneration)
				require.Contains(t, condition.Message, reconcileErr.Error())
				require.NotContains(t, condition.Message, statusErr.Error())
				persisted := &ledgerv1alpha1.Credentials{}
				require.NoError(t, baseClient.Get(t.Context(), client.ObjectKeyFromObject(credentials), persisted))
				if statusFails {
					require.Empty(t, persisted.Status.Conditions)
				} else {
					require.Equal(t, "Error", persisted.Status.Phase)
					persistedCondition := meta.FindStatusCondition(persisted.Status.Conditions, failure.conditionType)
					require.NotNil(t, persistedCondition)
					require.Equal(t, condition.Status, persistedCondition.Status)
					require.Equal(t, condition.Reason, persistedCondition.Reason)
					require.Equal(t, condition.Message, persistedCondition.Message)
					require.Equal(t, condition.ObservedGeneration, persistedCondition.ObservedGeneration)
					require.WithinDuration(t, condition.LastTransitionTime.Time.UTC().Truncate(time.Second), persistedCondition.LastTransitionTime.Time, 0)
				}
				var secrets corev1.SecretList
				require.NoError(t, baseClient.List(t.Context(), &secrets))
				require.Len(t, secrets.Items, 1)
				require.Equal(t, canonical.Data, secrets.Items[0].Data)
			})
		}
	}
}
