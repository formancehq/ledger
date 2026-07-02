package controller

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
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
		}, fmt.Errorf("exec failed: %w (stdout: %s) (stderr: %s)", err, strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
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
//
// tlsMode is the TLS_MODE currently in effect on the running pods (read from
// the existing StatefulSet before the operator updates it); ledgerctl is
// configured accordingly so its connection matches what the pod's gRPC server
// expects.
func raftScaleDown(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	ledger *ledgerv1alpha1.LedgerService, currentReplicas, desiredReplicas int32, tlsMode string,
) error {
	logger := log.FromContext(ctx)
	grpcPort := ledger.Spec.GrpcPort
	pod0 := podName(ledger.Name, 0)
	container := "ledger"
	serverAddr := podSelfServerAddr(headlessServiceName(ledger.Name), grpcPort)

	// Transfer leadership to node 1 (pod-0) so the leader is never among the removed nodes.
	logger.Info("transferring Raft leadership to node 1 before scale-down")
	result, err := podExec(ctx, cfg, clientset, ledger.Namespace, pod0, container,
		ledgerctlCommand(serverAddr, tlsMode, "cluster", "transfer-leader", "1"),
	)
	if err != nil {
		// If already leader on node 1, that's fine — check for known benign messages.
		if result != nil && isAlreadyLeader(result.Stderr) {
			logger.Info("node 1 is already the leader")
		} else {
			return fmt.Errorf("transferring leadership to node 1: %w", err)
		}
	}

	// Partition nodes-to-remove into never-joined, crashed (force), and alive (normal).
	// Never-joined nodes (Pending, not scheduled) are skipped entirely.
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
		pod := podName(ledger.Name, int(ordinal))

		if neverJoined := isPodNeverReady(ctx, clientset, ledger.Namespace, pod); neverJoined {
			logger.Info("pod was never ready, skipping Raft removal (node never joined cluster)",
				"nodeID", nodeID,
				"podOrdinal", ordinal,
			)

			continue
		}

		crashed := isPodCrashed(ctx, clientset, ledger.Namespace, pod)
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
		if err := removeNode(ctx, cfg, clientset, ledger.Namespace, pod0, container, serverAddr, tlsMode, n.nodeID, true); err != nil {
			return err
		}
	}

	// Remove alive nodes normally (highest ordinal first, already sorted).
	for _, n := range aliveNodes {
		if err := removeNode(ctx, cfg, clientset, ledger.Namespace, pod0, container, serverAddr, tlsMode, n.nodeID, false); err != nil {
			return err
		}
	}

	return nil
}

// removeNode executes ledgerctl cluster remove-node via pod exec. If force is
// true, --force is appended to bypass Raft consensus.
func removeNode(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	namespace, pod0, container, serverAddr, tlsMode string, nodeID int32, force bool,
) error {
	logger := log.FromContext(ctx)

	subArgs := []string{"cluster", "remove-node", strconv.Itoa(int(nodeID))}
	if force {
		subArgs = append(subArgs, "--force")
	}
	args := ledgerctlCommand(serverAddr, tlsMode, subArgs...)

	logger.Info("removing Raft node before scale-down",
		"nodeID", nodeID,
		"force", force,
	)

	result, err := podExec(ctx, cfg, clientset, namespace, pod0, container, args)
	if err != nil {
		// Idempotent: node already removed or never in cluster.
		if result != nil && (isNodeNotInCluster(result.Stderr) || isNodeNotInCluster(result.Stdout)) {
			logger.Info("node not in cluster, skipping",
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

// ledgerctlCommand builds a shell command that runs ledgerctl with the cluster
// secret for authentication. The command is wrapped in sh -c so that env vars
// like $CLUSTER_SECRET, $TLS_CA_CERT_FILE, $POD_NAME and $POD_NAMESPACE are
// expanded at runtime inside the pod.
//
// serverAddr is the host:port the client should dial. For in-pod execs the
// caller must pass a DNS name that the server certificate is issued for (the
// pod's own headless DNS, not 127.0.0.1 — server certs typically only cover
// service/headless SANs).
//
// tlsMode must reflect what the target pod's gRPC server is actually running
// (TLS_MODE env on the running container); when "required" or "optional" the
// CA cert mounted in the pod is used for server verification, otherwise the
// connection is plaintext.
func ledgerctlCommand(serverAddr, tlsMode string, args ...string) []string {
	// Wrap each caller-supplied arg in single quotes so shell metacharacters
	// surfaced from CRD fields (e.g. mirror-aws-iam-region, mirror-dsn) or
	// Kubernetes Secrets (e.g. operator-built DSN carrying a passwordFrom
	// value) cannot be interpreted by /bin/sh -- defense-in-depth even when
	// the producer already URL-encodes the value. serverAddr and the
	// TLS-flag fragment stay unquoted because they contain intentional
	// shell-expanded variables ($POD_NAME, $POD_NAMESPACE, $TLS_CA_CERT_FILE);
	// both are produced by trusted internal helpers, never from user input.
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = shellSingleQuote(arg)
	}

	cmd := fmt.Sprintf(`./ledgerctl %s --server "%s" %s --auth-token "$CLUSTER_SECRET"`,
		strings.Join(quoted, " "), serverAddr, ledgerctlTLSFlag(tlsMode))

	return []string{"/bin/sh", "-c", cmd}
}

// shellSingleQuote returns s wrapped in single quotes safe for /bin/sh -c.
// A single quote inside the string is encoded as `'\”` (close, escaped
// literal, reopen), the canonical POSIX-safe escape.
func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// podSelfServerAddr is the host:port a ledger container should dial to reach
// its own gRPC listener via a name covered by the TLS cert. Using the pod's
// headless DNS (rather than 127.0.0.1) avoids SAN mismatches when TLS is on,
// because operator-issued certs typically cover the headless SANs only.
//
// The returned string contains shell variables ($POD_NAME, $POD_NAMESPACE)
// that the runtime container is expected to have via the downward API.
func podSelfServerAddr(headlessSvc string, grpcPort int32) string {
	return fmt.Sprintf("$POD_NAME.%s.$POD_NAMESPACE.svc.cluster.local:%d", headlessSvc, grpcPort)
}

// ledgerctlTLSFlag returns the TLS-related ledgerctl flag matching the gRPC
// server's TLS_MODE. A required/optional server needs a TLS client; anything
// else (disabled, unknown, bootstrap) must use --insecure to avoid sending a
// TLS ClientHello to a plaintext listener — or vice versa, hitting a TLS
// listener with HTTP/2 in clear, which surfaces as "error reading server
// preface" / "connection reset by peer".
func ledgerctlTLSFlag(tlsMode string) string {
	if tlsMode == tlsModeRequired || tlsMode == tlsModeOptional {
		return `--tls-ca-cert "$TLS_CA_CERT_FILE"`
	}

	return "--insecure"
}

// isPodNeverReady returns true if the pod has never been ready: not found,
// still Pending (not scheduled), or no container has ever started.
// These pods could never have joined the Raft cluster.
func isPodNeverReady(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) bool {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		// Not found → never existed, never joined.
		return kerrors.IsNotFound(err)
	}

	// Pending pods have never started.
	if pod.Status.Phase == corev1.PodPending {
		return true
	}

	// If the pod exists but no container has ever started, it never joined.
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.RestartCount > 0 || cs.Ready || cs.State.Running != nil || cs.State.Terminated != nil || cs.LastTerminationState.Running != nil || cs.LastTerminationState.Terminated != nil {
			return false
		}
	}

	// No container statuses at all means the pod was never scheduled.
	return len(pod.Status.ContainerStatuses) == 0
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

// deleteScaledDownPVCs deletes PVCs for pod ordinals that no longer exist after
// a scale-down. Only PVC-backed volumes are considered; hostPath volumes have
// no PVC to clean up. This must be called after the StatefulSet update so the
// pods are already terminated.
func deleteScaledDownPVCs(ctx context.Context, clientset kubernetes.Interface,
	namespace, stsName string, previousReplicas, currentReplicas int32,
	volumeNames []string,
) error {
	logger := log.FromContext(ctx)
	pvcs := clientset.CoreV1().PersistentVolumeClaims(namespace)

	for ordinal := previousReplicas - 1; ordinal >= currentReplicas; ordinal-- {
		for _, vol := range volumeNames {
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
