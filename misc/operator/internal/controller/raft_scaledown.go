package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
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
//
// Crashed pods are force-removed first (bypassing Raft consensus) to restore
// quorum before attempting normal consensus-based removal of alive nodes.
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

	// Partition nodes-to-remove into crashed (force) and alive (normal).
	// Force-removing crashed nodes first restores quorum for subsequent
	// consensus-based removals.
	type nodeToRemove struct {
		ordinal int32
		nodeID  int32
		crashed bool
	}

	var (
		crashedNodes []nodeToRemove
		aliveNodes   []nodeToRemove
	)
	for ordinal := currentReplicas - 1; ordinal >= desiredReplicas; ordinal-- {
		nodeID := ordinal + 1
		podName := podForOrdinal(ledger.Name, ordinal)
		crashed := isPodCrashed(ctx, clientset, ledger.Namespace, podName)
		n := nodeToRemove{ordinal: ordinal, nodeID: nodeID, crashed: crashed}
		if crashed {
			crashedNodes = append(crashedNodes, n)
		} else {
			aliveNodes = append(aliveNodes, n)
		}
		logger.Info("classified node for scale-down",
			"nodeID", nodeID,
			"podOrdinal", ordinal,
			"crashed", crashed,
		)
	}

	// Remove crashed nodes first with --force.
	for _, n := range crashedNodes {
		if err := removeNode(ctx, cfg, clientset, ledger.Namespace, pod0, container, grpcPort, n.nodeID, true); err != nil {
			return err
		}
	}

	// Remove alive nodes normally (highest ordinal first, already sorted).
	for _, n := range aliveNodes {
		if err := removeNode(ctx, cfg, clientset, ledger.Namespace, pod0, container, grpcPort, n.nodeID, false); err != nil {
			return err
		}
	}

	return nil
}

// removeNode executes ledgerctl cluster remove-node via pod exec. If force is
// true, --force is appended to bypass Raft consensus.
func removeNode(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	namespace, pod0, container string, grpcPort, nodeID int32, force bool,
) error {
	logger := log.FromContext(ctx)

	args := []string{
		"./ledgerctl", "cluster", "remove-node", fmt.Sprintf("%d", nodeID),
		"--server", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--insecure",
	}
	if force {
		args = append(args, "--force")
	}

	logger.Info("removing Raft node before scale-down",
		"nodeID", nodeID,
		"force", force,
	)

	result, err := podExec(ctx, cfg, clientset, namespace, pod0, container, args)
	if err != nil {
		// Idempotent: node already removed from cluster.
		if result != nil && isNodeNotInCluster(result.Stderr) {
			logger.Info("node already removed from cluster, skipping",
				"nodeID", nodeID,
			)
			return nil
		}
		return fmt.Errorf("removing node %d (force=%v): %w", nodeID, force, err)
	}

	logger.Info("successfully removed Raft node",
		"nodeID", nodeID,
		"force", force,
	)
	return nil
}

// podForOrdinal returns the pod name for a given StatefulSet ordinal.
func podForOrdinal(stsName string, ordinal int32) string {
	return fmt.Sprintf("%s-%d", stsName, ordinal)
}

// isPodCrashed returns true if the pod is permanently unreachable: not found,
// in Failed phase, or has a container in CrashLoopBackOff/Error/OOMKilled state.
// Pending and Running pods are considered alive (they may recover).
func isPodCrashed(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) bool {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		// Pod not found → already deleted or never scheduled.
		if kerrors.IsNotFound(err) {
			return true
		}
		// API error: treat as alive (don't force-remove on transient failure).
		return false
	}

	if pod.Status.Phase == corev1.PodFailed {
		return true
	}

	// Check container statuses for crash indicators.
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			reason := cs.State.Waiting.Reason
			if reason == "CrashLoopBackOff" || reason == "Error" || reason == "ErrImagePull" || reason == "ImagePullBackOff" {
				return true
			}
		}
		if cs.State.Terminated != nil {
			reason := cs.State.Terminated.Reason
			if reason == "OOMKilled" || reason == "Error" || cs.State.Terminated.ExitCode != 0 {
				return true
			}
		}
	}

	return false
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
