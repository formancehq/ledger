package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const eventSinkFinalizer = "ledger.formance.com/event-sink-cleanup"

// EventSinkReconciler owns only runtime sinks stamped with the EventSink UID.
type EventSinkReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Config    *rest.Config
	Clientset kubernetes.Interface
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=eventsinks,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=eventsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=eventsinks/finalizers,verbs=update
// +kubebuilder:rbac:groups=ledger.formance.com,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create

func (r *EventSinkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var sink ledgerv1alpha1.EventSink
	if err := r.Get(ctx, req.NamespacedName, &sink); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// The finalizer must exist before any Ledger mutation, including an ambiguous add.
	if sink.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(&sink, eventSinkFinalizer) {
		controllerutil.AddFinalizer(&sink, eventSinkFinalizer)

		return ctrl.Result{}, r.Update(ctx, &sink)
	}

	var cluster ledgerv1alpha1.Cluster
	clusterKey := types.NamespacedName{Namespace: sink.Namespace, Name: sink.Spec.ClusterRef.Name}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) && !sink.DeletionTimestamp.IsZero() {
			// A deleted Cluster can leave its StatefulSet running briefly. Wait for
			// the runtime to disappear before releasing cleanup ownership.
			var sts appsv1.StatefulSet
			stsKey := types.NamespacedName{Namespace: sink.Namespace, Name: resourceName(sink.Spec.ClusterRef.Name)}
			if stsErr := r.Get(ctx, stsKey, &sts); stsErr == nil {
				return ctrl.Result{RequeueAfter: sinkRequeueInterval}, nil
			} else if !apierrors.IsNotFound(stsErr) {
				return ctrl.Result{}, stsErr
			}
			controllerutil.RemoveFinalizer(&sink, eventSinkFinalizer)

			return ctrl.Result{}, r.Update(ctx, &sink)
		}

		return r.sinkFailure(ctx, &sink, "ClusterUnavailable", fmt.Errorf("getting Cluster %q: %w", clusterKey, err), sinkDriftCheckInterval)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		if sink.DeletionTimestamp.IsZero() {
			return r.sinkFailure(ctx, &sink, "ClusterDeleting", fmt.Errorf("cluster %q is deleting", cluster.Name), sinkDriftCheckInterval)
		}
	}
	applyDefaults(&cluster)

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: resourceName(cluster.Name)}, &sts); err != nil {
		if apierrors.IsNotFound(err) && !sink.DeletionTimestamp.IsZero() {
			controllerutil.RemoveFinalizer(&sink, eventSinkFinalizer)

			return ctrl.Result{}, r.Update(ctx, &sink)
		}

		return r.sinkFailure(ctx, &sink, "WaitingForCluster", fmt.Errorf("getting StatefulSet: %w", err), sinkRequeueInterval)
	}
	desiredReplicas := int32(3)
	if cluster.Spec.Replicas != nil {
		desiredReplicas = *cluster.Spec.Replicas
	}
	if sts.Status.ReadyReplicas != desiredReplicas || !rolloutConverged(&sts) {
		return r.sinkFailure(ctx, &sink, "WaitingForCluster", errors.New("waiting for Ledger StatefulSet rollout"), sinkRequeueInterval)
	}

	exec := func(args ...string) (string, error) {
		execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
		defer cancel()
		delegate := ClusterReconciler{Client: r.Client, Config: r.Config, Clientset: r.Clientset}

		return delegate.ledgerctlExecOutput(execCtx, cluster.Namespace, cluster.Name, podName(cluster.Name, 0), cluster.Spec.GrpcPort, args...)
	}
	stdout, err := exec("events", "list", "--json")
	if err != nil {
		return r.sinkFailure(ctx, &sink, "ListFailed", err, sinkRequeueInterval)
	}
	actual, err := parseActualEventSinks(stdout)
	if err != nil {
		return r.sinkFailure(ctx, &sink, "ListFailed", err, sinkRequeueInterval)
	}
	observed, exists := actual[sink.Name]
	if exists && observed.controllerID == string(sink.UID) {
		sink.Status.Cursor = observed.cursor
		sink.Status.Error = observed.deliveryError
	} else {
		sink.Status.Cursor = 0
		sink.Status.Error = ""
	}

	action, err := reconcileSinkRuntime(&sink, observed, exists, exec)
	if err != nil {
		interval := sinkRequeueInterval
		if action == sinkActionConflict {
			interval = sinkDriftCheckInterval
		}

		return r.sinkFailure(ctx, &sink, string(action), err, interval)
	}
	if !sink.DeletionTimestamp.IsZero() {
		if action == sinkActionAbsent {
			controllerutil.RemoveFinalizer(&sink, eventSinkFinalizer)

			return ctrl.Result{}, r.Update(ctx, &sink)
		}

		return ctrl.Result{RequeueAfter: sinkRequeueInterval}, nil
	}
	if action != sinkActionSynced {
		return r.sinkFailure(ctx, &sink, string(action), errors.New("waiting for runtime sink confirmation"), sinkRequeueInterval)
	}

	meta.SetStatusCondition(&sink.Status.Conditions, metav1.Condition{
		Type: "Synced", Status: metav1.ConditionTrue, Reason: "Configured",
		Message: "runtime sink configuration matches this EventSink", ObservedGeneration: sink.Generation,
	})
	meta.SetStatusCondition(&sink.Status.Conditions, sinkDeliveryCondition(observed, sink.Generation))
	if err := r.persistSinkStatus(ctx, &sink); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: sinkDriftCheckInterval}, nil
}

func sinkDeliveryCondition(observed actualEventSink, generation int64) metav1.Condition {
	condition := metav1.Condition{Type: "Delivering", ObservedGeneration: generation}
	switch {
	case !observed.hasStatus:
		condition.Status = metav1.ConditionUnknown
		condition.Reason = "StatusUnavailable"
		condition.Message = "Ledger has not reported delivery status for this sink"
	case observed.deliveryError != "":
		condition.Status = metav1.ConditionFalse
		condition.Reason = "RuntimeError"
		condition.Message = observed.deliveryError
	default:
		condition.Status = metav1.ConditionTrue
		condition.Reason = "NoError"
		condition.Message = "Ledger reports no delivery error"
	}

	return condition
}

type sinkRuntimeAction string

const (
	sinkActionSynced   sinkRuntimeAction = "Configured"
	sinkActionAdded    sinkRuntimeAction = "Creating"
	sinkActionRemoved  sinkRuntimeAction = "Updating"
	sinkActionAbsent   sinkRuntimeAction = "Absent"
	sinkActionConflict sinkRuntimeAction = "NameConflict"
	sinkActionFailed   sinkRuntimeAction = "MutationFailed"
)

// reconcileSinkRuntime never infers ownership from a matching name or config.
// A failed command is retried from a fresh list because its Raft commit may
// already have succeeded while the response was lost.
func reconcileSinkRuntime(sink *ledgerv1alpha1.EventSink, actual actualEventSink, exists bool, exec ledgerctlSinkExec) (sinkRuntimeAction, error) {
	owner := string(sink.UID)
	if exists && actual.controllerID != owner {
		if !sink.DeletionTimestamp.IsZero() {
			// The CR no longer owns this name; leave the foreign sink alone.
			return sinkActionAbsent, nil
		}

		return sinkActionConflict, fmt.Errorf("runtime sink %q is owned by another controller", sink.Name)
	}
	if !sink.DeletionTimestamp.IsZero() {
		if !exists {
			return sinkActionAbsent, nil
		}
		_, err := exec("events", "remove-sink", "--name", sink.Name, "--controller-id", owner)
		if err != nil {
			return sinkActionFailed, err
		}

		return sinkActionRemoved, nil
	}
	if exists {
		if eventSinksEqual(desiredEventSink(sink), actual) {
			return sinkActionSynced, nil
		}
		_, err := exec("events", "remove-sink", "--name", sink.Name, "--controller-id", owner)
		if err != nil {
			return sinkActionFailed, err
		}

		return sinkActionRemoved, nil
	}
	args := append(addNATSSinkArgs(desiredEventSink(sink)), "--controller-id", owner)
	if _, err := exec(args...); err != nil {
		return sinkActionFailed, err
	}

	return sinkActionAdded, nil
}

func (r *EventSinkReconciler) sinkFailure(ctx context.Context, sink *ledgerv1alpha1.EventSink, reason string, err error, interval time.Duration) (ctrl.Result, error) {
	if sink.DeletionTimestamp.IsZero() {
		meta.SetStatusCondition(&sink.Status.Conditions, metav1.Condition{
			Type: "Synced", Status: metav1.ConditionFalse, Reason: reason,
			Message: err.Error(), ObservedGeneration: sink.Generation,
		})
		if statusErr := r.persistSinkStatus(ctx, sink); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
	}

	return ctrl.Result{RequeueAfter: interval}, nil
}

func (r *EventSinkReconciler) persistSinkStatus(ctx context.Context, sink *ledgerv1alpha1.EventSink) error {
	var live ledgerv1alpha1.EventSink
	if err := r.Get(ctx, client.ObjectKeyFromObject(sink), &live); err != nil {
		return err
	}
	if reflect.DeepEqual(live.Status, sink.Status) {
		return nil
	}
	sink.ResourceVersion = live.ResourceVersion

	return r.Status().Update(ctx, sink)
}

func (r *EventSinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.EventSink{}).
		Complete(r)
}
