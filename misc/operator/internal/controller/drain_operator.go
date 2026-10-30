package controller

import (
	"context"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func (r *ClusterReconciler) PreflightDrainBarrier(ctx context.Context, cluster *ledgerv1alpha1.Cluster, tlsMode string) error {
	drain, err := r.drainCluster(ctx, cluster, tlsMode)
	if err != nil {
		return err
	}

	return preflightDrain(ctx, drain)
}

// EnsureDrainBarrier is the operator's controlled-shutdown gate. Call it with
// a stable epoch before reducing the StatefulSet replica count. A true result
// certifies that every member captured by the marker has locally applied and
// persisted the cutoff. Errors and false results must leave Pods running.
// The epoch is supplied by the operator workflow that initiates shutdown; no
// user-facing Cluster field or automatic upgrade trigger is defined here.
func (r *ClusterReconciler) EnsureDrainBarrier(ctx context.Context, cluster *ledgerv1alpha1.Cluster, epoch, tlsMode string) (bool, error) {
	drain, err := r.drainCluster(ctx, cluster, tlsMode)
	if err != nil {
		return false, err
	}

	return ensureDrain(ctx, drain, epoch)
}

// EnsureDrainReleased reverses a canceled shutdown. It returns true only
// after every original member has durably applied the Raft release marker.
func (r *ClusterReconciler) EnsureDrainReleased(ctx context.Context, cluster *ledgerv1alpha1.Cluster, epoch, tlsMode string) (bool, error) {
	drain, err := r.drainCluster(ctx, cluster, tlsMode)
	if err != nil {
		return false, err
	}

	return ensureDrainReleased(ctx, drain, epoch)
}

// drainCluster addresses the Pods of the StatefulSet the Cluster controls:
// every per-member exec first checks that its target Pod is controlled by
// that StatefulSet, so a Pod that merely holds a derived name is never
// drained or released.
func (r *ClusterReconciler) drainCluster(ctx context.Context, cluster *ledgerv1alpha1.Cluster, tlsMode string) (*ledgerctlDrainCluster, error) {
	sts, err := fetchClusterStatefulSet(ctx, r.Client, cluster)
	if err != nil {
		return nil, err
	}

	return &ledgerctlDrainCluster{
		cluster:   cluster,
		tlsMode:   tlsMode,
		config:    r.Config,
		clientset: r.Clientset,
		exec:      ownedPodExec(sts, podExecWithTimeout),
	}, nil
}
