package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	conditionSinksSynced = "SinksSynced"
	sinkRequeueInterval  = 5 * time.Second
)

type ledgerctlSinkExec func(args ...string) (string, error)

func (r *ClusterReconciler) handleSinkReconcile(ctx context.Context, cluster *ledgerv1alpha1.Cluster, baseResult ctrl.Result) ctrl.Result {
	if cluster.Spec.Sinks == nil {
		meta.RemoveStatusCondition(&cluster.Status.Conditions, conditionSinksSynced)

		return baseResult
	}

	ready, err := r.clusterReadyForSinkReconcile(ctx, cluster)
	if err != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               conditionSinksSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "Error",
			Message:            err.Error(),
			ObservedGeneration: cluster.Generation,
		})

		return earlierRequeue(baseResult, ctrl.Result{RequeueAfter: sinkRequeueInterval})
	}
	if !ready {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               conditionSinksSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "WaitingForCluster",
			Message:            "waiting for the StatefulSet rollout to converge before reconciling event sinks",
			ObservedGeneration: cluster.Generation,
		})

		return earlierRequeue(baseResult, ctrl.Result{RequeueAfter: sinkRequeueInterval})
	}

	pod0 := podName(cluster.Name, 0)
	exec := func(args ...string) (string, error) {
		execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
		defer cancel()

		return r.ledgerctlExecOutput(execCtx, cluster.Namespace, cluster.Name, pod0, cluster.Spec.GrpcPort, args...)
	}

	synced, err := reconcileEventSinks(cluster, exec)
	if err != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               conditionSinksSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "Error",
			Message:            err.Error(),
			ObservedGeneration: cluster.Generation,
		})
		ctrl.LoggerFrom(ctx).Error(err, "event sink reconciliation failed", "cluster", cluster.Name)

		return earlierRequeue(baseResult, ctrl.Result{RequeueAfter: sinkRequeueInterval})
	}

	if !synced {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               conditionSinksSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "InProgress",
			Message:            "event sink set changed; awaiting convergence",
			ObservedGeneration: cluster.Generation,
		})

		return earlierRequeue(baseResult, ctrl.Result{RequeueAfter: sinkRequeueInterval})
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionSinksSynced,
		Status:             metav1.ConditionTrue,
		Reason:             "Synced",
		Message:            "declared event sink set is configured in Ledger",
		ObservedGeneration: cluster.Generation,
	})

	return baseResult
}

func (r *ClusterReconciler) clusterReadyForSinkReconcile(ctx context.Context, cluster *ledgerv1alpha1.Cluster) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      resourceName(cluster.Name),
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return false, fmt.Errorf("getting StatefulSet before event sink reconcile: %w", err)
	}

	desiredReplicas := int32(3)
	if cluster.Spec.Replicas != nil {
		desiredReplicas = *cluster.Spec.Replicas
	}

	return sts.Status.ReadyReplicas == desiredReplicas && rolloutConverged(sts), nil
}

func (r *ClusterReconciler) ledgerctlExecOutput(ctx context.Context, namespace, serviceName, pod string, grpcPort int32, args ...string) (string, error) {
	tlsMode, err := fetchTLSMode(ctx, r.Client, namespace, resourceName(serviceName))
	if err != nil {
		return "", fmt.Errorf("resolving TLS mode for Cluster %q: %w", serviceName, err)
	}

	serverAddr := podSelfServerAddr(headlessServiceName(serviceName), grpcPort)
	cmd := ledgerctlCommand(serverAddr, tlsMode, args...)

	res, err := podExec(ctx, r.Config, r.Clientset, namespace, pod, ledgerContainer, cmd)
	if err != nil {
		return "", fmt.Errorf("ledgerctl %s: %w", args[0], err)
	}

	return res.Stdout, nil
}

func reconcileEventSinks(cluster *ledgerv1alpha1.Cluster, exec ledgerctlSinkExec) (bool, error) {
	desired := desiredEventSinks(cluster.Spec.Sinks)

	stdout, err := exec("events", "list", "--json")
	if err != nil {
		return false, err
	}

	actual, err := parseActualEventSinks(stdout)
	if err != nil {
		return false, err
	}

	diff := diffEventSinks(desired, actual, cluster.Status.AppliedSinks)
	if len(diff.conflict) > 0 {
		return false, fmt.Errorf("event sink name conflict: %v already exist with a different configuration and are not operator-owned", diff.conflict)
	}

	oldApplied := cluster.Status.AppliedSinks
	var created, dropped []string
	defer func() {
		cluster.Status.AppliedSinks = nextAppliedSinks(oldApplied, created, dropped)
	}()

	changed := false
	for _, sink := range diff.toCreate {
		if _, err := exec(addNATSSinkArgs(sink)...); err != nil {
			return false, err
		}
		created = append(created, sink.name)
		changed = true
	}

	for _, name := range diff.toDrop {
		if _, exists := actual[name]; exists {
			if _, err := exec(removeEventSinkArgs(name)...); err != nil && !isLedgerNotFound(err) {
				return false, err
			}
			changed = true
		}
		dropped = append(dropped, name)
	}

	return !changed, nil
}

func earlierRequeue(left, right ctrl.Result) ctrl.Result {
	switch {
	case left.IsZero():
		return right
	case right.IsZero():
		return left
	case left.Requeue:
		return left
	case right.Requeue:
		return right
	case left.RequeueAfter <= right.RequeueAfter:
		return left
	default:
		return right
	}
}
