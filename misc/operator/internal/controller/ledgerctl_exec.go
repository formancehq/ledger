package controller

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func execLedgerctlOutput(ctx context.Context, current client.Client, config *rest.Config, clientset kubernetes.Interface, exec ledgerctlExec, cluster *ledgerv1alpha1.Cluster, grpcPort int32, args ...string) (string, error) {
	sts, err := fetchClusterStatefulSet(ctx, current, cluster)
	if err != nil {
		return "", fmt.Errorf("resolving the StatefulSet of Cluster %q: %w", cluster.Name, err)
	}

	serverAddr := podSelfServerAddr(headlessServiceName(cluster.Name), grpcPort)
	cmd := ledgerctlCommand(serverAddr, currentTLSModeFromStatefulSet(sts), args...)

	res, err := ownedPodExec(sts, exec)(ctx, config, clientset, cluster.Namespace, podName(cluster.Name, 0), ledgerContainer, cmd)
	if err != nil {
		return "", fmt.Errorf("ledgerctl %s: %w", args[0], err)
	}

	return res.Stdout, nil
}
