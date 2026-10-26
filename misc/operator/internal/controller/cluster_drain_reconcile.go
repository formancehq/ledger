package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	// ClusterDrainAnnotation requests a live, write-blocked Ledger cluster.
	ClusterDrainAnnotation = "ledger.formance.com/drain"
	drainPhasePreflighting = "Preflighting"
	drainPhaseDraining     = "Draining"
	drainPhaseDrained      = "Drained"
	drainPhaseReleasing    = "Releasing"
	drainRequeueInterval   = 2 * time.Second
)

type ensureClusterDrain func(context.Context, *ledgerv1alpha1.Cluster, string, string) (bool, error)
type preflightClusterDrain func(context.Context, *ledgerv1alpha1.Cluster, string) error

// reconcileAnnotatedDrain keeps the epoch in status before sending a marker,
// and persists the release intent before sending a release. Both transitions
// survive operator restarts and annotation changes during an in-flight RPC.
func (r *ClusterReconciler) reconcileAnnotatedDrain(
	ctx context.Context,
	cluster *ledgerv1alpha1.Cluster,
	preflight preflightClusterDrain,
	ensureBarrier, ensureRelease ensureClusterDrain,
) (ctrl.Result, error) {
	_, requested := cluster.Annotations[ClusterDrainAnnotation]
	state := cluster.Status.Drain
	if state == nil {
		if !requested {
			meta.RemoveStatusCondition(&cluster.Status.Conditions, "Drained")

			return ctrl.Result{}, nil
		}
		cluster.Status.Drain = &ledgerv1alpha1.DrainStatus{Epoch: uuid.NewString(), Phase: drainPhasePreflighting}
		setDrainCondition(cluster, metav1.ConditionFalse, "Preflighting", "Checking drain support on every cluster member")
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting drain intent: %w", err)
		}

		return ctrl.Result{Requeue: true}, nil
	}
	if state.Epoch == "" {
		return ctrl.Result{}, errors.New("drain status has no epoch")
	}
	if state.Phase == drainPhasePreflighting && !requested {
		// BeginDrain has never been called for this epoch, so no marker can
		// arrive after cancellation. In particular, an unsupported member
		// must not keep the operator stuck in a drain intent forever.
		cluster.Status.Drain = nil
		meta.RemoveStatusCondition(&cluster.Status.Conditions, "Drained")
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("clearing canceled drain preflight: %w", err)
		}

		return ctrl.Result{Requeue: true}, nil
	}
	if state.Phase == drainPhaseDrained && !requested {
		state.Phase = drainPhaseReleasing
		setDrainCondition(cluster, metav1.ConditionFalse, "Releasing", "Waiting for the cluster-wide drain release")
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting drain release intent: %w", err)
		}

		return ctrl.Result{Requeue: true}, nil
	}
	if state.Phase != drainPhasePreflighting && state.Phase != drainPhaseDraining && state.Phase != drainPhaseDrained && state.Phase != drainPhaseReleasing {
		return ctrl.Result{}, fmt.Errorf("unknown drain phase %q", state.Phase)
	}
	if state.Phase == drainPhaseDrained {
		setDrainCondition(cluster, metav1.ConditionTrue, "BarrierComplete", "All members durably applied the drain barrier")

		return ctrl.Result{}, nil
	}

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
		}

		return ctrl.Result{}, fmt.Errorf("reading drain workload: %w", err)
	}
	if sts.Spec.Replicas == nil || sts.Status.ReadyReplicas != *sts.Spec.Replicas || *sts.Spec.Replicas == 0 {
		return ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
	}
	liveCluster, err := drainClusterFromWorkload(cluster, sts)
	if err != nil {
		return ctrl.Result{}, err
	}
	tlsMode := currentTLSModeFromStatefulSet(sts)
	if state.Phase == drainPhasePreflighting {
		if err := preflight(ctx, liveCluster, tlsMode); err != nil {
			return ctrl.Result{}, fmt.Errorf("preflighting annotated drain: %w", err)
		}
		state.Phase = drainPhaseDraining
		setDrainCondition(cluster, metav1.ConditionFalse, "Draining", "Waiting for the cluster-wide drain barrier")
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting completed drain preflight: %w", err)
		}

		return ctrl.Result{Requeue: true}, nil
	}
	if state.Phase == drainPhaseReleasing {
		setDrainCondition(cluster, metav1.ConditionFalse, "Releasing", "Waiting for the cluster-wide drain release")
		ready, err := ensureRelease(ctx, liveCluster, state.Epoch, tlsMode)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("releasing annotated drain: %w", err)
		}
		if !ready {
			return ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
		}
		if requested {
			// A re-added annotation is a new request. Persist its fresh epoch
			// immediately after the old release so a retry cannot reuse it.
			cluster.Status.Drain = &ledgerv1alpha1.DrainStatus{Epoch: uuid.NewString(), Phase: drainPhasePreflighting}
			setDrainCondition(cluster, metav1.ConditionFalse, "Preflighting", "Checking drain support on every cluster member")
		} else {
			cluster.Status.Drain = nil
			meta.RemoveStatusCondition(&cluster.Status.Conditions, "Drained")
		}
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting released drain status: %w", err)
		}

		return ctrl.Result{Requeue: requested}, nil
	}

	setDrainCondition(cluster, metav1.ConditionFalse, "Draining", "Waiting for the cluster-wide drain barrier")
	ready, err := ensureBarrier(ctx, liveCluster, state.Epoch, tlsMode)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("applying annotated drain: %w", err)
	}
	if !ready {
		return ctrl.Result{RequeueAfter: drainRequeueInterval}, nil
	}
	if !requested {
		state.Phase = drainPhaseReleasing
		setDrainCondition(cluster, metav1.ConditionFalse, "Releasing", "Waiting for the cluster-wide drain release")
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting drain release intent: %w", err)
		}

		return ctrl.Result{Requeue: true}, nil
	}
	state.Phase = drainPhaseDrained
	setDrainCondition(cluster, metav1.ConditionTrue, "BarrierComplete", "All members durably applied the drain barrier")
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("persisting completed drain: %w", err)
	}

	return ctrl.Result{}, nil
}

// Drain reconciliation freezes normal StatefulSet updates, so transport must
// address the Pods that are actually running, even if the desired spec changes.
func drainClusterFromWorkload(cluster *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) (*ledgerv1alpha1.Cluster, error) {
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas == 0 {
		return nil, errors.New("drain workload has no replicas")
	}
	var grpcPort int32
	for _, container := range sts.Spec.Template.Spec.Containers {
		if container.Name != ledgerContainer {
			continue
		}
		for _, port := range container.Ports {
			if port.Name == "grpc" && port.Protocol == corev1.ProtocolTCP {
				grpcPort = port.ContainerPort

				break
			}
		}
	}
	if grpcPort <= 0 {
		return nil, errors.New("drain workload has no gRPC container port")
	}
	live := cluster.DeepCopy()
	replicas := *sts.Spec.Replicas
	live.Spec.Replicas = &replicas
	live.Spec.GrpcPort = grpcPort

	return live, nil
}

func setDrainCondition(cluster *ledgerv1alpha1.Cluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: "Drained", Status: status, Reason: reason, Message: message,
		ObservedGeneration: cluster.Generation,
	})
}
