package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// execResult holds stdout/stderr from a pod exec command.
type execResult struct {
	Stdout string
	Stderr string
}

type ledgerctlExec func(
	ctx context.Context,
	cfg *rest.Config,
	clientset kubernetes.Interface,
	namespace, podName, container string,
	command []string,
) (*execResult, error)

const scaleDownLeaderNodeID int32 = 1

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

// podExecWithTimeout runs podExec under its own ledgerExecTimeout. Every
// operator-owned in-pod ledgerctl invocation must be bounded by a per-exec
// deadline so a single blocked exec — a stuck SPDY stream or a ledgerctl
// process sitting in its OTEL shutdown flush — cannot hold the whole reconcile.
// The Ledger reconcile path wraps its calls at each call site; the Cluster
// scale-down path, which reused the outer reconcile context, goes through this
// helper.
func podExecWithTimeout(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	namespace, podName, container string, command []string,
) (*execResult, error) {
	execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
	defer cancel()

	return podExec(execCtx, cfg, clientset, namespace, podName, container, command)
}

// raftScaleDown removes Raft nodes before StatefulSet scale-down.
// Every removed ordinal must pass the Raft membership check, regardless of
// its current Pod incarnation. Nodes are removed sequentially within each group.
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
	ledger *ledgerv1alpha1.Cluster, currentReplicas, desiredReplicas int32, tlsMode string, exec ledgerctlExec,
) error {
	logger := log.FromContext(ctx)
	grpcPort := ledger.Spec.GrpcPort
	pod0 := podName(ledger, 0)
	container := "ledger"
	serverAddr := podSelfServerAddr(headlessServiceName(ledger), grpcPort)

	// Transfer leadership to node 1 (pod-0) so the leader is never among the removed nodes.
	logger.Info("transferring Raft leadership to node 1 before scale-down")
	result, err := exec(ctx, cfg, clientset, ledger.Namespace, pod0, container,
		ledgerctlCommand(serverAddr, tlsMode, "cluster", "transfer-leader", strconv.Itoa(int(scaleDownLeaderNodeID))),
	)
	if err != nil {
		// If already leader on node 1, that's fine — check for known benign messages.
		if result != nil && isAlreadyLeader(result.Stderr) {
			logger.Info("node 1 is already the leader")
		} else {
			return fmt.Errorf("transferring leadership to node 1: %w", err)
		}
	}

	// Partition nodes-to-remove into crashed (force) and alive (normal).
	// Pod status selects the removal mode; only Raft membership can establish
	// absence. A missing or Pending replacement may reuse a joined ordinal.
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
		pod := podName(ledger, int(ordinal))

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
		if err := removeNodeWithExec(ctx, cfg, clientset, ledger.Namespace, pod0, container, serverAddr, tlsMode, n.nodeID, true, exec); err != nil {
			return err
		}
	}

	// Remove alive nodes normally (highest ordinal first, already sorted).
	for _, n := range aliveNodes {
		if err := removeNodeWithExec(ctx, cfg, clientset, ledger.Namespace, pod0, container, serverAddr, tlsMode, n.nodeID, false, exec); err != nil {
			return err
		}
	}

	return nil
}

// removeNodeWithExec executes ledgerctl cluster remove-node via pod exec. If force
// is true, --force is appended to bypass Raft consensus.
func removeNodeWithExec(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	namespace, pod0, container, serverAddr, tlsMode string, nodeID int32, force bool,
	exec ledgerctlExec,
) error {
	logger := log.FromContext(ctx)
	present, err := raftNodePresent(
		ctx, cfg, clientset,
		namespace, pod0, container, serverAddr, tlsMode, nodeID, force,
		exec,
	)
	if err != nil {
		return fmt.Errorf("checking Raft membership before removing node %d: %w", nodeID, err)
	}

	if !present {
		logger.Info("Raft node already absent before scale-down removal; postcondition satisfied",
			"nodeID", nodeID,
			"force", force,
		)

		return nil
	}

	subArgs := []string{"cluster", "remove-node", strconv.Itoa(int(nodeID))}
	if force {
		subArgs = append(subArgs, "--force")
	}
	args := ledgerctlCommand(serverAddr, tlsMode, subArgs...)

	logger.Info("removing Raft node before scale-down",
		"nodeID", nodeID,
		"force", force,
	)

	_, err = exec(ctx, cfg, clientset, namespace, pod0, container, args)
	if err != nil {
		// The RPC can fail after ConfChangeRemoveNode has committed (for
		// example while its tombstone is still waiting on Pebble). Verify the
		// actual membership postcondition rather than matching CLI text, which
		// may be intentionally sanitized by the gRPC server.
		stillPresent, statusErr := raftNodePresent(
			ctx, cfg, clientset,
			namespace, pod0, container, serverAddr, tlsMode, nodeID, force,
			exec,
		)
		if statusErr != nil {
			return fmt.Errorf(
				"removing node %d (force=%v): %w; membership verification after failure also failed: %w",
				nodeID, force, err, statusErr,
			)
		}

		if !stillPresent {
			logger.Info("Raft node absent after remove-node error; committed postcondition satisfied",
				"nodeID", nodeID,
				"force", force,
				"removeError", err.Error(),
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

// raftNodePresent reports whether nodeID is in the current Raft membership.
// Normal removal uses leader routing. Force removal explicitly queries node 1,
// which raftScaleDown has already made the pod-0 leader, so the check remains a
// local raw Raft status read and never depends on a quorum-backed route during
// quorum-loss recovery. The leader-state check below still fails closed if
// leadership changes before removal.
func raftNodePresent(ctx context.Context, cfg *rest.Config, clientset kubernetes.Interface,
	namespace, pod0, container, serverAddr, tlsMode string, nodeID int32, force bool,
	exec ledgerctlExec,
) (bool, error) {
	statusArgs := []string{"cluster", "status"}
	if force {
		statusArgs = append(statusArgs, "--node-id", strconv.Itoa(int(scaleDownLeaderNodeID)))
	}
	statusArgs = append(statusArgs, "--json")
	args := ledgerctlCommand(serverAddr, tlsMode, statusArgs...)
	result, err := exec(ctx, cfg, clientset, namespace, pod0, container, args)
	if err != nil {
		return false, fmt.Errorf("executing ledgerctl cluster status: %w", err)
	}
	if result == nil {
		return false, errors.New("executing ledgerctl cluster status returned no result")
	}

	var state struct {
		State string `json:"state"`
		Nodes []struct {
			ID uint32 `json:"id"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(result.Stdout), &state); err != nil {
		return false, fmt.Errorf("decoding ledgerctl cluster status JSON: %w", err)
	}
	if !strings.EqualFold(state.State, "leader") {
		return false, fmt.Errorf("cluster status was served by a non-leader node (state %q)", state.State)
	}

	for _, member := range state.Nodes {
		if member.ID == uint32(nodeID) {
			return true, nil
		}
	}

	return false, nil
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
	// shell-expanded variables ($POD_NAME, $POD_NAMESPACE, $GRPC_PORT,
	// $TLS_CA_CERT_FILE);
	// both are produced by trusted internal helpers, never from user input.
	//
	// A "--" terminator ends flag parsing: every later arg is positional, which
	// is how callers pass a user-supplied value that may start with "-" (a
	// ledger name such as "-orders" or "--help"). The connection flags must
	// therefore be inserted before the terminator, not appended after it.
	head, tail := args, []string(nil)
	if i := slices.Index(args, "--"); i >= 0 {
		head, tail = args[:i], args[i:]
	}

	cmd := otelExecPrologue + fmt.Sprintf(`./ledgerctl %s --server "%s" %s --auth-token "$CLUSTER_SECRET"`,
		shellQuoteJoin(head), serverAddr, ledgerctlTLSFlag(tlsMode))
	if len(tail) > 0 {
		cmd += " " + shellQuoteJoin(tail)
	}

	return []string{"/bin/sh", "-c", cmd}
}

// shellQuoteJoin single-quotes each arg and joins them with spaces.
func shellQuoteJoin(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = shellSingleQuote(arg)
	}

	return strings.Join(quoted, " ")
}

// otelExecPrologue bridges the pod's go-libs OTEL_* env to the names ledgerctl
// (a standard OTEL SDK consumer) actually reads, so an in-pod exec exports
// traces correctly when a collector is configured and pays no cost when one is
// not.
//
// The pod carries go-libs' OTEL_* env — OTEL_TRACES_EXPORTER=otlp plus the
// endpoint/insecure/mode under the go-libs names OTEL_TRACES_EXPORTER_OTLP_*
// — but ledgerctl reads the OTEL-SDK-standard OTEL_EXPORTER_OTLP_ENDPOINT /
// OTEL_EXPORTER_OTLP_PROTOCOL, which the pod never sets. With the exporter
// selected but no standard endpoint resolvable, ledgerctl builds an OTLP
// exporter against the SDK default localhost:4317, and its deferred span flush
// blocks for the full 5s shutdown timeout on every invocation.
//
// So, when a traces endpoint is configured:
//   - expose it under the standard name, adding the http/https scheme the SDK
//     requires (derived from the go-libs insecure flag — a bare host:port makes
//     the SDK attempt TLS against a plaintext collector and hang the same 5s);
//   - translate the go-libs OTLP mode (grpc|http) to the standard
//     OTEL_EXPORTER_OTLP_PROTOCOL, so an HTTP collector isn't hit with the
//     default gRPC exporter (which would flush-fail and hang).
//
// The SDK is disabled entirely (flush becomes a no-op) when either traces are
// turned off at the Cluster level — go-libs injects OTEL_TRACES=false, a flag
// ledgerctl does not itself honor, so a stale/configured endpoint would
// otherwise still be exported — or no endpoint is configured at all. The
// go-libs var names are the ones this same operator injects in
// appendMonitoringEnvVars, so both ends stay in sync.
//
// A configured-but-unreachable collector can still spend up to the 5s flush
// budget on exit; ledgerExecTimeout is sized above that so it never surfaces as
// a spurious "context deadline exceeded".
const otelExecPrologue = `if [ "${OTEL_TRACES:-}" = "false" ] || [ -z "${OTEL_TRACES_EXPORTER_OTLP_ENDPOINT:-}" ]; then ` +
	`export OTEL_SDK_DISABLED=true; else ` +
	`case "$OTEL_TRACES_EXPORTER_OTLP_ENDPOINT" in ` +
	`http://*|https://*) export OTEL_EXPORTER_OTLP_ENDPOINT="$OTEL_TRACES_EXPORTER_OTLP_ENDPOINT" ;; ` +
	`*) if [ "${OTEL_TRACES_EXPORTER_OTLP_INSECURE:-}" = "true" ]; then ` +
	`export OTEL_EXPORTER_OTLP_ENDPOINT="http://$OTEL_TRACES_EXPORTER_OTLP_ENDPOINT"; else ` +
	`export OTEL_EXPORTER_OTLP_ENDPOINT="https://$OTEL_TRACES_EXPORTER_OTLP_ENDPOINT"; fi ;; ` +
	`esac; ` +
	`if [ -n "${OTEL_TRACES_EXPORTER_OTLP_MODE:-}" ]; then ` +
	`export OTEL_EXPORTER_OTLP_PROTOCOL="$OTEL_TRACES_EXPORTER_OTLP_MODE"; fi; fi; ` //nolint:dupword // closes two nested shell `if` blocks, not a duplicated word

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

// isPodCrashed returns true if the pod is permanently unreachable: not found,
// in Failed phase, or has a container in CrashLoopBackOff/Error/OOMKilled state.
// Pending and Running pods without a crash indicator are considered alive
// (they may recover).
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

// isAlreadyLeader checks whether the error output indicates the target is already
// the leader (transfer-leader is a no-op).
func isAlreadyLeader(stderr string) bool {
	lower := strings.ToLower(stderr)

	return strings.Contains(lower, "already the leader") ||
		strings.Contains(lower, "already leader")
}

const (
	// annotationScaledDownFrom, on the Cluster's StatefulSet, records the
	// replica count a scale-down started from while the PVCs of the removed
	// ordinals [spec.replicas, value) still need cleanup or are reported as
	// kept. It is written in the same update that lowers spec.replicas and
	// removed once no claim of the range is left (reconcileScaledDownPVCs).
	// A StatefulSet recreated for a VolumeClaimTemplate change starts without
	// it: the claims still pending then are neither deleted nor reported.
	annotationScaledDownFrom = "ledger.formance.com/scaled-down-from"

	// conditionScaleDownPVCsRetained is True while PVCs of removed ordinals are
	// kept because the Cluster's StatefulSet did not create them (reason
	// PVCsRetained), or while such claims make the operator refuse a scale-up
	// (reason ScaleUpBlocked). It is removed once none is left.
	conditionScaleDownPVCsRetained = "ScaleDownPVCsRetained"
)

// statefulSetReplicas returns sts.spec.replicas, defaulting to 1 like the API
// server.
func statefulSetReplicas(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas == nil {
		return 1
	}

	return *sts.Spec.Replicas
}

// scaledDownFrom returns the annotationScaledDownFrom record of sts, or 0 when
// there is none. A value that is not a positive integer is an invariant
// failure: the operator alone writes it.
func scaledDownFrom(sts *appsv1.StatefulSet) (int32, error) {
	value, ok := sts.Annotations[annotationScaledDownFrom]
	if !ok {
		return 0, nil
	}
	from, err := strconv.ParseInt(value, 10, 32)
	if err != nil || from < 1 {
		return 0, fmt.Errorf("invariant: StatefulSet %s/%s has an invalid %s annotation %q", sts.Namespace, sts.Name, annotationScaledDownFrom, value)
	}

	return int32(from), nil
}

// recordScaledDownFrom raises the annotationScaledDownFrom record of sts to
// previousReplicas, keeping a higher range still pending from an earlier
// scale-down.
func recordScaledDownFrom(sts *appsv1.StatefulSet, previousReplicas int32) error {
	from, err := scaledDownFrom(sts)
	if err != nil {
		return err
	}
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[annotationScaledDownFrom] = strconv.Itoa(int(max(from, previousReplicas)))

	return nil
}

// scaleUpBlockingClaims returns the PVCs that a scale-up of the live
// StatefulSet existing to desiredReplicas would remount although a scale-down
// removed their ordinal and they are still recorded for cleanup or reporting
// (annotationScaledDownFrom), whoever owns them. It returns nil when the
// replica count does not grow into that range. Claims of ordinals outside the
// record (for instance retained from a deleted Cluster) are intended reuse and
// never block.
func scaleUpBlockingClaims(ctx context.Context, clientset kubernetes.Interface, cluster *ledgerv1alpha1.Cluster,
	existing *appsv1.StatefulSet, desiredReplicas int32,
) ([]string, error) {
	from, err := scaledDownFrom(existing)
	if err != nil {
		return nil, err
	}
	live := statefulSetReplicas(existing)
	var present []string
	for ordinal := live; ordinal < min(desiredReplicas, from); ordinal++ {
		for _, vol := range pvcVolumeNames(&cluster.Spec.Persistence) {
			claimName := pvcName(cluster, vol, ordinal)
			_, err := clientset.CoreV1().PersistentVolumeClaims(cluster.Namespace).Get(ctx, claimName, metav1.GetOptions{})
			if kerrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("getting PVC %s: %w", claimName, err)
			}
			present = append(present, claimName)
		}
	}

	return present, nil
}

// deleteScaledDownPVCs deletes the PVCs of the pod ordinals removed by a
// scale-down, [currentReplicas, previousReplicas). Only PVC-backed volumes are
// considered; hostPath volumes have no PVC to clean up. This must be called
// after the StatefulSet update has requested termination of those pods.
//
// A PVC is looked up by its derived name "<volume>-<base>-<ordinal>", which
// proves nothing: with spec.resourceName that name can belong to someone else.
// sts is the live StatefulSet the Cluster controls, and a PVC is deleted only
// when scaledDownPVCMismatch finds it is one that StatefulSet's controller
// created for that ordinal. A PVC that does not match is never deleted: it is
// logged and returned in retained, for the caller to report. The delete
// carries a UID precondition, so a PVC replaced after it was inspected is
// retained too. An API error on one claim does not stop the others; the
// errors are returned joined, and the caller retries the whole range.
func deleteScaledDownPVCs(ctx context.Context, clientset kubernetes.Interface,
	cluster *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, previousReplicas, currentReplicas int32,
	volumeNames []string,
) (retained []string, err error) {
	if sts == nil || sts.UID == "" {
		return nil, errors.New("invariant: deleting scaled-down PVCs without the inspected Cluster StatefulSet")
	}
	logger := log.FromContext(ctx)
	pvcs := clientset.CoreV1().PersistentVolumeClaims(cluster.Namespace)

	var errs []error
	for ordinal := previousReplicas - 1; ordinal >= currentReplicas; ordinal-- {
		for _, vol := range volumeNames {
			claimName := pvcName(cluster, vol, ordinal)
			pvc, err := pvcs.Get(ctx, claimName, metav1.GetOptions{})
			if kerrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("getting PVC %s: %w", claimName, err))

				continue
			}
			if reason := scaledDownPVCMismatch(pvc, sts, podName(cluster, int(ordinal))); reason != "" {
				logger.Info("not deleting PVC of removed replica: not created by this Cluster's StatefulSet",
					"pvc", claimName, "ordinal", ordinal, "reason", reason)
				retained = append(retained, claimName+" ("+reason+")")

				continue
			}

			logger.Info("deleting PVC for removed replica", "pvc", claimName, "ordinal", ordinal)
			uid := pvc.UID
			err = pvcs.Delete(ctx, claimName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
			switch {
			case err == nil, kerrors.IsNotFound(err):
			case kerrors.IsConflict(err):
				// The PVC under the name was replaced after it was inspected: the
				// replacement was never checked, so it is not deleted.
				logger.Info("not deleting PVC of removed replica: replaced after its ownership was checked",
					"pvc", claimName, "ordinal", ordinal, "inspectedUID", uid)
				retained = append(retained, claimName+" (replaced after its ownership was checked)")
			default:
				errs = append(errs, fmt.Errorf("deleting PVC %s: %w", claimName, err))
			}
		}
	}

	return retained, errors.Join(errs...)
}

// scaledDownPVCMismatch returns why pvc, found under the derived name of
// ordinalPod's claim, is not a claim that sts created for that ordinal, or ""
// when it is. The policy follows what the StatefulSet controller writes on the
// claims it creates from a VolumeClaimTemplate:
//
//   - labels: it copies sts.spec.selector.matchLabels onto every claim, and the
//     selector is immutable, so the claim must carry all of them. They include
//     app.kubernetes.io/instance=<Cluster name>, so a claim of another
//     Cluster's StatefulSet does not match even under the same base name.
//   - ownerReferences: with persistentVolumeClaimRetentionPolicy Retain/Retain
//     (the default) the claim has none; whenDeleted=Delete adds sts itself and
//     whenScaled=Delete adds the condemned Pod. Any other reference, such as
//     another StatefulSet's UID, means another owner.
//
// A retained, ownerless claim left by an earlier StatefulSet of a Cluster with
// the same name and selector labels matches, which keeps a recreated Cluster's
// reuse of its retained PVCs intact, scale-down cleanup included. A retained
// claim from a Cluster with another name or other selector labels does not:
// the StatefulSet still mounts it, but scale-down never deletes it.
func scaledDownPVCMismatch(pvc *corev1.PersistentVolumeClaim, sts *appsv1.StatefulSet, ordinalPod string) string {
	if sts.Spec.Selector == nil || len(sts.Spec.Selector.MatchLabels) == 0 {
		return "the StatefulSet has no selector labels to match"
	}
	// Sorted keys keep the reported reason, and so the condition message,
	// stable across passes.
	for _, key := range slices.Sorted(maps.Keys(sts.Spec.Selector.MatchLabels)) {
		value := sts.Spec.Selector.MatchLabels[key]
		if got, ok := pvc.Labels[key]; !ok || got != value {
			return fmt.Sprintf("label %s=%q does not match the StatefulSet selector %q", key, got, value)
		}
	}
	for _, ref := range pvc.OwnerReferences {
		switch {
		case ref.Kind == "StatefulSet" && ref.UID == sts.UID:
		case ref.Kind == "Pod" && ref.Name == ordinalPod:
		default:
			return fmt.Sprintf("owned by %s/%s (uid %s)", ref.Kind, ref.Name, ref.UID)
		}
	}

	return ""
}
