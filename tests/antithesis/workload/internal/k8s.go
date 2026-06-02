package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
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
