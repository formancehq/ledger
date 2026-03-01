package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// execResult holds stdout/stderr from a pod exec command.
type execResult struct {
	Stdout string
	Stderr string
}

// podExec runs a command inside a container using the Kubernetes exec API.
func podExec(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	namespace, podName, container string, command []string,
) (*execResult, error) {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("creating SPDY executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return &execResult{
			Stdout: stdout.String(),
			Stderr: stderr.String(),
		}, fmt.Errorf("exec failed: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}

	return &execResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}, nil
}

// raftScaleDown removes Raft nodes before StatefulSet scale-down.
// It removes nodes from highest ordinal to desired, sequentially.
// Leadership is transferred to node 1 (pod-0) first to ensure the leader isn't being removed.
func raftScaleDown(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	ledger *ledgerv1alpha1.LedgerService, currentReplicas, desiredReplicas int32,
) error {
	logger := log.FromContext(ctx)
	grpcPort := ledger.Spec.Config.GrpcPort
	pod0 := fmt.Sprintf("%s-0", ledger.Name)
	container := "ledger"

	// Transfer leadership to node 1 (pod-0) so the leader is never among the removed nodes.
	logger.Info("transferring Raft leadership to node 1 before scale-down")
	result, err := podExec(ctx, cfg, clientset, ledger.Namespace, pod0, container, []string{
		"./ledgerctl", "cluster", "transfer-leader", "1",
		"--server", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--insecure",
	})
	if err != nil {
		// If already leader on node 1, that's fine — check for known benign messages.
		if result != nil && isAlreadyLeader(result.Stderr) {
			logger.Info("node 1 is already the leader")
		} else {
			return fmt.Errorf("transferring leadership to node 1: %w", err)
		}
	}

	// Remove nodes from highest ordinal down to desiredReplicas.
	// Node ID = ordinal + 1, so pod-4 has node ID 5.
	for ordinal := currentReplicas - 1; ordinal >= desiredReplicas; ordinal-- {
		nodeID := ordinal + 1
		logger.Info("removing Raft node before scale-down",
			"nodeID", nodeID,
			"podOrdinal", ordinal,
		)

		result, err := podExec(ctx, cfg, clientset, ledger.Namespace, pod0, container, []string{
			"./ledgerctl", "cluster", "remove-node", fmt.Sprintf("%d", nodeID),
			"--server", fmt.Sprintf("127.0.0.1:%d", grpcPort),
			"--insecure",
		})
		if err != nil {
			// Idempotent: node already removed from cluster.
			if result != nil && isNodeNotInCluster(result.Stderr) {
				logger.Info("node already removed from cluster, skipping",
					"nodeID", nodeID,
				)
				continue
			}
			return fmt.Errorf("removing node %d: %w", nodeID, err)
		}

		logger.Info("successfully removed Raft node",
			"nodeID", nodeID,
		)
	}

	return nil
}

// isNodeNotInCluster checks whether the error output indicates the node is already
// absent from the Raft cluster (idempotent removal).
func isNodeNotInCluster(stderr string) bool {
	lower := strings.ToLower(stderr)
	return strings.Contains(lower, "not in cluster") ||
		strings.Contains(lower, "not a member") ||
		strings.Contains(lower, "not found")
}

// isAlreadyLeader checks whether the error output indicates the target is already
// the leader (transfer-leader is a no-op).
func isAlreadyLeader(stderr string) bool {
	lower := strings.ToLower(stderr)
	return strings.Contains(lower, "already the leader") ||
		strings.Contains(lower, "already leader")
}

// volumeClaimNames are the VolumeClaimTemplate names used by the StatefulSet.
// PVCs are named "{volumeName}-{stsName}-{ordinal}".
var volumeClaimNames = []string{"wal", "data"}

// deleteScaledDownPVCs deletes PVCs for pod ordinals that no longer exist after
// a scale-down. This must be called after the StatefulSet update so the pods are
// already terminated.
func deleteScaledDownPVCs(ctx context.Context, clientset kubernetes.Interface,
	namespace, stsName string, previousReplicas, currentReplicas int32,
) error {
	logger := log.FromContext(ctx)
	pvcs := clientset.CoreV1().PersistentVolumeClaims(namespace)

	for ordinal := previousReplicas - 1; ordinal >= currentReplicas; ordinal-- {
		for _, vol := range volumeClaimNames {
			pvcName := fmt.Sprintf("%s-%s-%d", vol, stsName, ordinal)
			logger.Info("deleting PVC for removed replica",
				"pvc", pvcName,
				"ordinal", ordinal,
			)
			if err := pvcs.Delete(ctx, pvcName, metav1.DeleteOptions{}); err != nil {
				// Already gone — idempotent.
				if strings.Contains(err.Error(), "not found") {
					logger.Info("PVC already deleted, skipping", "pvc", pvcName)
					continue
				}
				return fmt.Errorf("deleting PVC %s: %w", pvcName, err)
			}
		}
	}

	return nil
}
