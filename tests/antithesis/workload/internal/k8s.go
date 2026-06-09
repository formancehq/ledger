package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"golang.org/x/net/context"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// LedgerServiceGVR is the GroupVersionResource for the LedgerService CRD.
var LedgerServiceGVR = schema.GroupVersionResource{
	Group:    "ledger.formance.com",
	Version:  "v1alpha1",
	Resource: "ledgerservices",
}

// OddReplicas are the valid replica counts (always odd, min 3, max 7).
var OddReplicas = []int64{3, 5, 7}

// NewK8sClient creates a Kubernetes dynamic client from in-cluster config.
func NewK8sClient() (dynamic.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building k8s config: %w", err)
	}

	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating k8s dynamic client: %w", err)
	}

	return client, nil
}

// NewKubeClientset creates a typed Kubernetes clientset for pod and STS ops.
func NewKubeClientset() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building k8s config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating k8s clientset: %w", err)
	}

	return clientset, nil
}

// LedgerServiceName is the LedgerService CR name used in the Antithesis k8s
// manifests. Drives the StatefulSet and pod naming (ledger-0, ledger-1, ...).
const LedgerServiceName = "ledger"

// LedgerServiceNamespace returns the namespace to use, from POD_NAMESPACE env or "default".
func LedgerServiceNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	return "default"
}

// GetCurrentReplicas reads the current replica count from the LedgerService CR.
func GetCurrentReplicas(ctx context.Context, lsClient dynamic.ResourceInterface, name string) (int64, error) {
	obj, err := lsClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}

	replicas, found, _ := UnstructuredNestedInt64(obj.Object, "spec", "replicas")
	if !found {
		return 3, nil
	}

	return replicas, nil
}

// PatchReplicas patches the LedgerService replica count.
func PatchReplicas(ctx context.Context, lsClient dynamic.ResourceInterface, name string, replicas int64) error {
	patch := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	_, err := lsClient.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})

	return err
}

// WaitForVoters polls the cluster state until the expected voter count is
// reached or the timeout expires. Returns true if converged.
func WaitForVoters(ctx context.Context, clusterClient clusterpb.ClusterServiceClient, expected int64, timeout time.Duration, details Details) bool {
	deadline := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			log.Printf("scaling: timed out waiting for %d voters", expected)
			assert.Sometimes(false, "scaling should converge within timeout", details.With(Details{"timeout": timeout.String()}))

			return false
		case <-time.After(5 * time.Second):
		}

		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		if err != nil {
			log.Printf("scaling: cluster state unavailable: %s", err)

			continue
		}

		if state.GetLeader() == 0 {
			continue
		}

		var voterCount int64
		for _, n := range state.GetNodes() {
			if n.GetSuffrage() == "Voter" {
				voterCount++
			}
		}

		if voterCount == expected {
			log.Printf("scaling: cluster converged (%d voters, leader=%d)", voterCount, state.GetLeader())
			assert.Reachable("scaling converged to target", details.With(Details{
				"voters": voterCount,
				"leader": state.GetLeader(),
			}))

			return true
		}

		log.Printf("scaling: %d/%d voters (leader=%d)", voterCount, expected, state.GetLeader())
	}
}

// UnstructuredNestedInt64 extracts a nested int64 from an unstructured object.
func UnstructuredNestedInt64(obj map[string]any, fields ...string) (int64, bool, error) {
	val, found, err := nestedField(obj, fields...)
	if !found || err != nil {
		return 0, found, err
	}

	switch v := val.(type) {
	case int64:
		return v, true, nil
	case float64:
		return int64(v), true, nil
	case json.Number:
		i, err := v.Int64()

		return i, true, err
	}

	return 0, false, fmt.Errorf("unexpected type %T for %v", val, fields)
}

func nestedField(obj map[string]any, fields ...string) (any, bool, error) {
	var current any = obj
	for _, f := range fields {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false, nil
		}

		current, ok = m[f]
		if !ok {
			return nil, false, nil
		}
	}

	return current, true, nil
}

// LedgerPodName returns the StatefulSet pod name for the given ordinal.
func LedgerPodName(ordinal int) string {
	return fmt.Sprintf("%s-%d", LedgerServiceName, ordinal)
}

// PodOrdinal extracts the ordinal from a pod name (e.g. "ledger-2" -> 2).
// Returns -1 if the name does not match.
func PodOrdinal(podName string) int {
	prefix := LedgerServiceName + "-"
	if !strings.HasPrefix(podName, prefix) {
		return -1
	}
	n, err := strconv.Atoi(podName[len(prefix):])
	if err != nil {
		return -1
	}
	return n
}

// ListLedgerPods returns the names of ledger pods sorted by ordinal.
func ListLedgerPods(ctx context.Context, clientset kubernetes.Interface) ([]string, error) {
	pods, err := clientset.CoreV1().Pods(LedgerServiceNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/instance=" + LedgerServiceName,
	})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(pods.Items))
	for _, p := range pods.Items {
		if PodOrdinal(p.Name) >= 0 {
			names = append(names, p.Name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		return PodOrdinal(names[i]) < PodOrdinal(names[j])
	})
	return names, nil
}

// DeletePod removes the given pod with grace=0 so the StatefulSet recreates it
// quickly. Suitable for fault-injection scenarios; not for graceful shutdowns.
func DeletePod(ctx context.Context, clientset kubernetes.Interface, name string) error {
	zero := int64(0)
	return clientset.CoreV1().Pods(LedgerServiceNamespace()).Delete(ctx, name, metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
	})
}

// WaitForPodGone polls until the pod with the given UID is gone or its UID
// has changed (signalling the StatefulSet has recreated it).
func WaitForPodGone(ctx context.Context, clientset kubernetes.Interface, name string, originalUID types.UID, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-time.After(2 * time.Second):
		}
		pod, err := clientset.CoreV1().Pods(LedgerServiceNamespace()).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true
		}
		if err == nil && pod.UID != originalUID {
			return true
		}
	}
}

// WaitForPodReady polls until the pod is in Ready condition or timeout expires.
func WaitForPodReady(ctx context.Context, clientset kubernetes.Interface, name string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-time.After(2 * time.Second):
		}
		pod, err := clientset.CoreV1().Pods(LedgerServiceNamespace()).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			continue
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				return true
			}
		}
	}
}

// WaitForStatefulSetReady polls until the StatefulSet reports the expected
// ready replica count.
func WaitForStatefulSetReady(ctx context.Context, clientset kubernetes.Interface, name string, expected int32, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-time.After(2 * time.Second):
		}
		sts, err := clientset.AppsV1().StatefulSets(LedgerServiceNamespace()).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			continue
		}
		if sts.Status.ReadyReplicas == expected && sts.Status.CurrentRevision == sts.Status.UpdateRevision {
			return true
		}
	}
}

// GetPodUID returns the UID of the named pod, or "" if not found.
func GetPodUID(ctx context.Context, clientset kubernetes.Interface, name string) (types.UID, error) {
	pod, err := clientset.CoreV1().Pods(LedgerServiceNamespace()).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return pod.UID, nil
}

// GetLeaderPodName returns the pod name hosting the current Raft leader,
// or an empty string if no leader is known.
func GetLeaderPodName(ctx context.Context, clusterClient clusterpb.ClusterServiceClient) (string, uint32, error) {
	state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return "", 0, err
	}
	leader := state.GetLeader()
	if leader == 0 {
		return "", 0, nil
	}
	// Node IDs in the Antithesis cluster are 1-based and match pod ordinal+1.
	return LedgerPodName(int(leader) - 1), leader, nil
}

// GetNonLeaderVoter returns the node ID of a non-leader voter, or 0 if none.
func GetNonLeaderVoter(ctx context.Context, clusterClient clusterpb.ClusterServiceClient) (uint32, error) {
	state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return 0, err
	}
	leader := state.GetLeader()
	for _, n := range state.GetNodes() {
		if n.GetId() != leader && n.GetSuffrage() == "Voter" {
			return n.GetId(), nil
		}
	}
	return 0, nil
}

// GetClusterConfig returns the ClusterConfig observed by the given node
// (0 = route to leader).
func GetClusterConfig(ctx context.Context, clusterClient clusterpb.ClusterServiceClient, nodeID uint32) (*commonpb.ClusterConfig, error) {
	state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: nodeID})
	if err != nil {
		return nil, err
	}
	return state.GetClusterConfig(), nil
}

// WaitForClusterConfig polls the leader's ClusterConfig until the predicate
// returns true or timeout expires.
func WaitForClusterConfig(ctx context.Context, clusterClient clusterpb.ClusterServiceClient, predicate func(*commonpb.ClusterConfig) bool, timeout time.Duration) bool {
	return WaitForClusterConfigOnNode(ctx, clusterClient, 0, predicate, timeout)
}

// WaitForClusterConfigOnNode polls a specific node's ClusterConfig until the
// predicate returns true. Pass nodeID=0 to route to the leader.
// Used to verify follower convergence after the leader has applied — Raft
// entries reach followers a few hundred ms later, so a tight poll absorbs the
// natural propagation delay without false-flagging an FSM divergence.
func WaitForClusterConfigOnNode(ctx context.Context, clusterClient clusterpb.ClusterServiceClient, nodeID uint32, predicate func(*commonpb.ClusterConfig) bool, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		cfg, err := GetClusterConfig(ctx, clusterClient, nodeID)
		if err == nil && cfg != nil && predicate(cfg) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-time.After(1 * time.Second):
		}
	}
}

// PatchCacheRotationThreshold updates spec.cache.rotationThreshold on the CR.
// Triggers a rolling restart; convergence verified via WaitForClusterConfig.
func PatchCacheRotationThreshold(ctx context.Context, lsClient dynamic.ResourceInterface, name string, threshold int32) error {
	patch := []byte(fmt.Sprintf(`{"spec":{"cache":{"rotationThreshold":%d}}}`, threshold))
	_, err := lsClient.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// PatchBloomExpectedKeys updates spec.bloom.<category>.expectedKeys on the CR.
// Category is the camelCase JSON key (volumes, metadata, references, ledgers,
// boundaries, transactions, sinkConfigs, numscriptVersions, numscriptContents,
// ledgerMetadata).
func PatchBloomExpectedKeys(ctx context.Context, lsClient dynamic.ResourceInterface, name, category string, expectedKeys int64) error {
	patch := []byte(fmt.Sprintf(`{"spec":{"bloom":{%q:{"expectedKeys":%d}}}}`, category, expectedKeys))
	_, err := lsClient.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// PatchBloomFPRate updates spec.bloom.<category>.fpRate. Rate is a stringified
// float in (0,1) per the operator validation rules.
func PatchBloomFPRate(ctx context.Context, lsClient dynamic.ResourceInterface, name, category, rate string) error {
	patch := []byte(fmt.Sprintf(`{"spec":{"bloom":{%q:{"fpRate":%q}}}}`, category, rate))
	_, err := lsClient.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}
