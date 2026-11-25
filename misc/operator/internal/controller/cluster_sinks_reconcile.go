package controller

import (
	"context"
	"time"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	sinkRequeueInterval    = 5 * time.Second
	sinkDriftCheckInterval = time.Minute
)

// ledgerctlExecOutput runs a ledgerctl command inside pod-0 of the Cluster.
// Like LedgerReconciler.ledgerctlExecOutput, it execs only into a pod-0
// controlled by the Cluster's own StatefulSet.
func (r *ClusterReconciler) ledgerctlExecOutput(ctx context.Context, cluster *ledgerv1alpha1.Cluster, grpcPort int32, args ...string) (string, error) {
	return execLedgerctlOutput(ctx, r.Client, r.Config, r.Clientset, podExec, cluster, grpcPort, args...)
}
