package controller

import (
	"context"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// EnsureDrainBarrier is the operator's controlled-shutdown gate. Call it with
// a stable epoch before reducing the StatefulSet replica count. A true result
// certifies that every member captured by the marker has locally applied and
// persisted the cutoff. Errors and false results must leave Pods running.
// The epoch is supplied by the operator workflow that initiates shutdown; no
// user-facing Cluster field or automatic upgrade trigger is defined here.
func (r *ClusterReconciler) EnsureDrainBarrier(ctx context.Context, cluster *ledgerv1alpha1.Cluster, epoch, tlsMode string) (bool, error) {
	return ensureDrain(ctx, &ledgerctlDrainCluster{
		cluster:   cluster,
		tlsMode:   tlsMode,
		config:    r.Config,
		clientset: r.Clientset,
		exec:      podExecWithTimeout,
	}, epoch)
}
