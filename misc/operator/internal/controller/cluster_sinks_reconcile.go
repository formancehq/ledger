package controller

import (
	"context"
	"fmt"
	"time"
)

const (
	sinkRequeueInterval    = 5 * time.Second
	sinkDriftCheckInterval = time.Minute
)

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
