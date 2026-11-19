package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestLedgerResolveClusterPort(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		spec map[string]any
		want int32
	}{
		{name: "omitted", want: 8888},
		{name: "zero", spec: map[string]any{"grpcPort": int64(0)}, want: 8888},
		{name: "custom", spec: map[string]any{"grpcPort": int64(9443)}, want: 9443},
		{name: "obsolete nested field ignored", spec: map[string]any{"config": map[string]any{"grpcPort": int64(9999)}}, want: 8888},
		{name: "typed field wins", spec: map[string]any{"grpcPort": int64(9443), "config": map[string]any{"grpcPort": int64(9999)}}, want: 9443},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newLedgerOwnershipFixture(t)
			if test.spec != nil {
				fixture.cluster.Object["spec"] = test.spec
			}
			fixture.ledger.Generation = 7
			reconciler, _ := fixture.reconciler(nil, nil, nil)

			cluster, port, err := reconciler.resolveEndpoint(t.Context(), fixture.ledger)
			require.NoError(t, err)
			require.Equal(t, fixture.cluster.GetUID(), cluster.UID)
			require.Equal(t, test.want, port)
			condition := meta.FindStatusCondition(fixture.ledger.Status.Conditions, conditionEndpointResolved)
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionTrue, condition.Status)
			require.Equal(t, "Resolved", condition.Reason)
			require.EqualValues(t, 7, condition.ObservedGeneration)

			cluster, port, err = reconciler.resolveCluster(t.Context(), fixture.ledger.Namespace, fixture.ledger.Spec.ClusterRef)
			require.NoError(t, err)
			require.Equal(t, fixture.cluster.GetUID(), cluster.UID)
			require.Equal(t, test.want, port)
		})
	}
}

func TestLedgerResolveEndpointRequiresRunningCluster(t *testing.T) {
	t.Parallel()

	for _, phase := range []any{nil, "Pending", "Failed", int64(1)} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			t.Parallel()
			fixture := newLedgerOwnershipFixture(t)
			fixture.cluster.Object["status"] = map[string]any{"phase": phase}
			fixture.cluster.Object["spec"] = map[string]any{"grpcPort": int64(9443)}
			reconciler, _ := fixture.reconciler(nil, nil, nil)

			cluster, port, err := reconciler.resolveEndpoint(t.Context(), fixture.ledger)
			require.ErrorContains(t, err, "is not Running")
			require.Nil(t, cluster)
			require.Zero(t, port)
			condition := meta.FindStatusCondition(fixture.ledger.Status.Conditions, conditionEndpointResolved)
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, "ClusterNotReady", condition.Reason)

			if phase == nil || phase == "Pending" || phase == "Failed" {
				cluster, port, err = reconciler.resolveCluster(t.Context(), fixture.ledger.Namespace, fixture.ledger.Spec.ClusterRef)
				require.NoError(t, err)
				require.NotNil(t, cluster)
				require.EqualValues(t, 9443, port)
			}
		})
	}
}

func TestLedgerResolveClusterFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		getErr    error
		malformed bool
		reason    string
		message   string
	}{
		{name: "missing", getErr: apierrors.NewNotFound(clusterGVR.GroupResource(), "main"), reason: "ClusterNotFound", message: "get Cluster"},
		{name: "API failure", getErr: errors.New("API unavailable"), reason: "ClusterNotFound", message: "API unavailable"},
		{name: "invalid port", malformed: true, reason: "ClusterUnreadable", message: "decoding Cluster"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newLedgerOwnershipFixture(t)
			if test.malformed {
				fixture.cluster.Object["spec"] = map[string]any{"grpcPort": "not-a-port"}
			}
			reconciler, _ := fixture.reconciler(nil, nil, nil)
			if test.getErr != nil {
				reconciler.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor("get", "clusters", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, test.getErr
				})
			}

			cluster, port, err := reconciler.resolveEndpoint(t.Context(), fixture.ledger)
			require.ErrorContains(t, err, test.message)
			require.Nil(t, cluster)
			require.Zero(t, port)
			condition := meta.FindStatusCondition(fixture.ledger.Status.Conditions, conditionEndpointResolved)
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, test.reason, condition.Reason)

			cluster, port, err = reconciler.resolveCluster(t.Context(), fixture.ledger.Namespace, fixture.ledger.Spec.ClusterRef)
			require.ErrorContains(t, err, test.message)
			require.Nil(t, cluster)
			require.Zero(t, port)
			if test.getErr != nil {
				require.ErrorIs(t, err, test.getErr)
			}
		})
	}
}

func TestLedgerReconcileUsesConfiguredClusterPort(t *testing.T) {
	t.Parallel()

	for _, port := range []int64{0, 9443} {
		for _, deleting := range []bool{false, true} {
			t.Run(fmt.Sprintf("port=%d/deleting=%t", port, deleting), func(t *testing.T) {
				t.Parallel()
				fixture := newLedgerOwnershipFixture(t)
				fixture.cluster.Object["spec"] = map[string]any{"grpcPort": port}
				if deleting {
					fixture.ledger.DeletionTimestamp = new(metav1.Now())
					fixture.cluster.Object["status"] = map[string]any{"phase": "Pending"}
				}
				statefulSet := fixture.ownedStatefulSet()
				statefulSet.Spec.Template.Spec.Containers = []corev1.Container{{Name: ledgerContainer, Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeRequired}}}}
				var commands [][]string
				exec := func(ctx context.Context, _ *rest.Config, _ kubernetes.Interface, namespace, pod, container string, command []string) (*execResult, error) {
					require.Equal(t, fixture.ledger.Namespace, namespace)
					require.Equal(t, podName("main", 0), pod)
					require.Equal(t, ledgerContainer, container)
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.WithinDuration(t, time.Now().Add(ledgerExecTimeout), deadline, time.Second)
					commands = append(commands, command)

					return &execResult{}, nil
				}
				reconciler, apiClient := fixture.reconciler([]client.Object{statefulSet}, []runtime.Object{ownedPod(statefulSet, 0)}, exec)

				_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fixture.ledger)})
				require.NoError(t, err)
				require.Len(t, commands, 1)
				require.Equal(t, []string{"/bin/sh", "-c"}, commands[0][:2])
				wantPort := port
				if wantPort == 0 {
					wantPort = 8888
				}
				require.Contains(t, commands[0][2], fmt.Sprintf(`--server "$POD_NAME.ledger-main-headless.$POD_NAMESPACE.svc.cluster.local:%d"`, wantPort))
				require.Contains(t, commands[0][2], `--tls-ca-cert "$TLS_CA_CERT_FILE"`)
				require.Contains(t, commands[0][2], `--auth-token "$CLUSTER_SECRET"`)
				stored := &ledgerv1alpha1.Ledger{}
				if deleting {
					require.Contains(t, commands[0][2], "'ledgers' 'delete' '--yes'")
					require.True(t, apierrors.IsNotFound(apiClient.Get(t.Context(), client.ObjectKeyFromObject(fixture.ledger), stored)))
				} else {
					require.Contains(t, commands[0][2], "'ledgers' 'create'")
					require.NoError(t, apiClient.Get(t.Context(), client.ObjectKeyFromObject(fixture.ledger), stored))
					require.Equal(t, ledgerv1alpha1.LedgerPhaseReady, stored.Status.Phase)
				}
			})
		}
	}
}
