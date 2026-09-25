package controller

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func testEventSink() *ledgerv1alpha1.EventSink {
	return &ledgerv1alpha1.EventSink{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", UID: types.UID("sink-uid")},
		Spec: ledgerv1alpha1.EventSinkSpec{
			ClusterRef: ledgerv1alpha1.EventSinkClusterRef{Name: "cluster"},
			NATS:       ledgerv1alpha1.EventSinkNATSSpec{URL: "nats://nats:4222", Topic: "ledger.events"},
		},
	}
}

func TestEventSinkDeletionWaitsForOrphanedStatefulSet(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	sink := testEventSink()
	sink.Namespace = "test"
	sink.Finalizers = []string{eventSinkFinalizer}
	now := metav1.Now()
	sink.DeletionTimestamp = &now
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName("cluster"), Namespace: "test"}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sink, sts).Build()
	r := &EventSinkReconciler{Client: kube, Scheme: scheme}
	key := types.NamespacedName{Name: sink.Name, Namespace: sink.Namespace}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	var stored ledgerv1alpha1.EventSink
	require.NoError(t, kube.Get(context.Background(), key, &stored))
	require.Contains(t, stored.Finalizers, eventSinkFinalizer)

	require.NoError(t, kube.Delete(context.Background(), sts))
	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	if err := kube.Get(context.Background(), key, &stored); err == nil {
		require.NotContains(t, stored.Finalizers, eventSinkFinalizer)
	}
}

func TestEventSinkListIncludesOwnershipAndDeliveryStatus(t *testing.T) {
	t.Parallel()
	actual, err := parseActualEventSinks(`{"sinks":[{"name":"primary","controllerId":"sink-uid","nats":{"url":"nats://nats:4222","topic":"ledger.events"}}],"sinkStatuses":[{"sinkName":"primary","cursor":"42","error":{"message":"connection refused"}}]}`)
	require.NoError(t, err)
	require.Equal(t, "sink-uid", actual["primary"].controllerID)
	require.True(t, actual["primary"].hasStatus)
	require.Equal(t, uint64(42), actual["primary"].cursor)
	require.Equal(t, "connection refused", actual["primary"].deliveryError)
	require.True(t, eventSinksEqual(desiredEventSink(testEventSink()), actual["primary"]))
}

func TestEventSinkDeliveryConditionRequiresObservedStatus(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		status  string
		want    metav1.ConditionStatus
		reason  string
		message string
	}{
		{name: "missing status", want: metav1.ConditionUnknown, reason: "StatusUnavailable", message: "Ledger has not reported delivery status for this sink"},
		{name: "observed without error", status: `{"sinkName":"primary","cursor":"0"}`, want: metav1.ConditionTrue, reason: "NoError", message: "Ledger reports no delivery error"},
		{name: "observed with error", status: `{"sinkName":"primary","error":{"message":"unsupported sink type"}}`, want: metav1.ConditionFalse, reason: "RuntimeError", message: "unsupported sink type"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			actual, err := parseActualEventSinks(`{"sinks":[{"name":"primary","controllerId":"sink-uid","nats":{"url":"nats://nats:4222","topic":"ledger.events"}}],"sinkStatuses":[` + test.status + `]}`)
			require.NoError(t, err)
			condition := sinkDeliveryCondition(actual["primary"], 3)
			require.Equal(t, test.want, condition.Status)
			require.Equal(t, test.reason, condition.Reason)
			require.Equal(t, test.message, condition.Message)
			require.Equal(t, int64(3), condition.ObservedGeneration)
		})
	}
}

func TestEventSinkNeverMutatesAnExternalName(t *testing.T) {
	t.Parallel()
	sink := testEventSink()
	actual := actualEventSink{kind: "nats", nats: desiredEventSink(sink)}
	action, err := reconcileSinkRuntime(sink, actual, true, func(...string) (string, error) {
		t.Fatal("external sink was mutated")

		return "", nil
	})
	require.Error(t, err)
	require.Equal(t, sinkActionConflict, action)

	now := metav1.Now()
	sink.DeletionTimestamp = &now
	action, err = reconcileSinkRuntime(sink, actual, true, func(...string) (string, error) {
		t.Fatal("external sink was deleted")

		return "", nil
	})
	require.NoError(t, err)
	require.Equal(t, sinkActionAbsent, action)
}

func TestEventSinkRecoversAnAmbiguousAddByReadingOwner(t *testing.T) {
	t.Parallel()
	sink := testEventSink()
	var calls [][]string
	exec := func(args ...string) (string, error) {
		calls = append(calls, slices.Clone(args))

		return "", errors.New("response lost")
	}
	action, err := reconcileSinkRuntime(sink, actualEventSink{}, false, exec)
	require.Error(t, err)
	require.Equal(t, sinkActionFailed, action)
	require.Equal(t, "--controller-id", calls[0][len(calls[0])-2])
	require.Equal(t, "sink-uid", calls[0][len(calls[0])-1])

	actual := actualEventSink{kind: "nats", controllerID: "sink-uid", nats: desiredEventSink(sink)}
	action, err = reconcileSinkRuntime(sink, actual, true, func(...string) (string, error) {
		t.Fatal("confirmed sink was added again")

		return "", nil
	})
	require.NoError(t, err)
	require.Equal(t, sinkActionSynced, action)
}

func TestEventSinkUpdateAndDeletionUseGuardedRemove(t *testing.T) {
	t.Parallel()
	sink := testEventSink()
	actual := actualEventSink{kind: "nats", controllerID: "sink-uid", nats: desiredEventSink(sink)}
	actual.nats.topic = "old.topic"
	var calls [][]string
	exec := func(args ...string) (string, error) {
		calls = append(calls, slices.Clone(args))

		return "", nil
	}
	action, err := reconcileSinkRuntime(sink, actual, true, exec)
	require.NoError(t, err)
	require.Equal(t, sinkActionRemoved, action)
	require.Equal(t, []string{"events", "remove-sink", "--name", "primary", "--controller-id", "sink-uid"}, calls[0])

	now := metav1.Now()
	sink.DeletionTimestamp = &now
	actual.nats.topic = sink.Spec.NATS.Topic
	action, err = reconcileSinkRuntime(sink, actual, true, exec)
	require.NoError(t, err)
	require.Equal(t, sinkActionRemoved, action)
	require.Equal(t, calls[0], calls[1])
	action, err = reconcileSinkRuntime(sink, actualEventSink{}, false, exec)
	require.NoError(t, err)
	require.Equal(t, sinkActionAbsent, action)
	require.Len(t, calls, 2)
}
