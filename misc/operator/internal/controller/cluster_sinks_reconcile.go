package controller

import (
	"context"
	"fmt"
	"time"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	sinkRequeueInterval    = 5 * time.Second
	sinkDriftCheckInterval = time.Minute
)

// ledgerctlExecOutput runs a ledgerctl command inside pod-0 of the Cluster,
// dialing the pod's own headless DNS name derived from the Cluster base name.
// Like LedgerReconciler.ledgerctlExecOutput, it execs only into a pod-0
// controlled by the Cluster's own StatefulSet.
func (r *ClusterReconciler) ledgerctlExecOutput(ctx context.Context, cluster *ledgerv1alpha1.Cluster, grpcPort int32, args ...string) (string, error) {
	sts, err := fetchClusterStatefulSet(ctx, r.Client, cluster)
	if err != nil {
		return "", fmt.Errorf("resolving the StatefulSet of Cluster %q: %w", cluster.Name, err)
	}

	serverAddr := podSelfServerAddr(headlessServiceName(cluster), grpcPort)
	cmd := ledgerctlCommand(serverAddr, currentTLSModeFromStatefulSet(sts), args...)

	res, err := ownedPodExec(sts, podExec)(ctx, r.Config, r.Clientset, cluster.Namespace, podName(cluster, 0), ledgerContainer, cmd)
	if err != nil {
		return "", fmt.Errorf("ledgerctl %s: %w", args[0], err)
	}

	return res.Stdout, nil
}
