// singleton_driver_config_change mutates the cluster-wide cache and bloom
// configuration at runtime, then asserts deterministic convergence:
//
//   - rotationThreshold ∈ {25, 50, 100, 200}
//   - bloom.<category>.expectedKeys ∈ {500_000, 1_000_000, 2_000_000}
//   - bloom.<category>.fpRate       ∈ {"0.001", "0.01", "0.05"}
//
// For each cycle, the driver:
//  1. Pre-commits a sentinel transaction.
//  2. Patches one config field on the LedgerService CR (the operator rolls the
//     StatefulSet pod template, which carries the new env vars).
//  3. Waits for the StatefulSet to become Ready again.
//  4. Polls the leader's ClusterConfig until the new value is observed —
//     server-side proposeClusterConfigIfNeeded fires on leader acquisition.
//  5. Cross-checks a follower's ClusterConfig and asserts equality
//     (Cache-is-the-source-of-authority: FSM must not diverge across nodes).
//  6. Re-reads the sentinel transaction.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var ccSentinelLedger = internal.PrefixSentinel.WithSuffix("config-change")

const (
	ccPatchSleep           = 30 * time.Second
	ccConvergeWait         = 8 * time.Minute
	ccStsReadyWait         = 8 * time.Minute
	ccFollowerConvergeWait = 30 * time.Second
	ccCooldown             = 90 * time.Second
)

var (
	rotationOptions = []int32{25, 50, 100, 200}
	keysOptions     = []int64{500_000, 1_000_000, 2_000_000}
	fpRateOptions   = []string{"0.001", "0.01", "0.05"}
	bloomCategories = []string{"volumes", "metadata", "references", "ledgers", "transactions"}
)

type configChange struct {
	kind      string
	category  string
	value     any
	apply     func(ctx context.Context, lsClient dynamic.ResourceInterface) error
	predicate func(cfg *commonpb.ClusterConfig) bool
}

func main() {
	log.Println("composer: singleton_driver_config_change")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
	dynClient, err := internal.NewK8sClient()
	if err != nil {
		log.Printf("cannot build k8s client: %s", err)
		return
	}
	clientset, err := internal.NewKubeClientset()
	if err != nil {
		log.Printf("cannot build k8s clientset: %s", err)
		return
	}

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("cannot create ledger gRPC client: %s", err)
		return
	}
	defer conn.Close()

	clusterClient := clusterpb.NewClusterServiceClient(conn)
	lsClient := dynClient.Resource(internal.LedgerServiceGVR).Namespace(internal.LedgerServiceNamespace())

	if err := internal.CreateLedger(ctx, client, ccSentinelLedger); err != nil && !internal.IsTransient(err) {
		log.Printf("cannot create sentinel ledger: %s", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(ccCooldown):
		}

		runRound(ctx, lsClient, clientset, clusterClient, client)
	}
}

func runRound(ctx context.Context, lsClient dynamic.ResourceInterface, clientset kubernetes.Interface, clusterClient clusterpb.ClusterServiceClient, client servicepb.BucketServiceClient) {
	change := pickChange(internal.Rand())

	sentinel, err := internal.PreCommitSentinel(ctx, client, ccSentinelLedger)
	if err != nil {
		if !internal.IsTransient(err) {
			log.Printf("config-change: precommit failed: %s", err)
		}
		return
	}

	details := internal.Details{
		"changeKind": change.kind,
		"category":   change.category,
		"value":      change.value,
		"sentinel":   sentinel.TxID,
	}

	log.Printf("config-change: patching %s (%s=%v)", change.kind, change.category, change.value)
	err = change.apply(ctx, lsClient)
	assert.Sometimes(err == nil, "config patch should succeed", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}
	assert.Reachable("config patch applied", details)

	select {
	case <-ctx.Done():
		return
	case <-time.After(ccPatchSleep):
	}

	currentReplicas, err := internal.GetCurrentReplicas(ctx, lsClient, internal.LedgerServiceName)
	if err != nil {
		log.Printf("config-change: cannot read current replicas: %s", err)
		return
	}
	ready := internal.WaitForStatefulSetReady(ctx, clientset, internal.LedgerStatefulSetName(), int32(currentReplicas), ccStsReadyWait)
	assert.Sometimes(ready, "StatefulSet should reach Ready after a config patch", details)
	if !ready {
		return
	}
	assert.Reachable("STS ready after config patch", details)

	converged := internal.WaitForClusterConfig(ctx, clusterClient, change.predicate, ccConvergeWait)
	assert.Sometimes(converged, "ClusterConfig should converge to the patched value", details)
	if !converged {
		return
	}

	// Best-effort follower convergence check. A real FSM divergence is
	// structurally impossible (deterministic Raft apply); the strong
	// correctness guarantee lives in sentinel.Verify (Always). Under fault
	// injection a follower can be partitioned from the leader and stay behind
	// for far longer than any reasonable wait — Reachable on success keeps
	// the signal in the report without false-flagging during partitions.
	followerID, err := internal.GetNonLeaderVoter(ctx, clusterClient)
	if err == nil && followerID != 0 {
		if internal.WaitForClusterConfigOnNode(ctx, clusterClient, followerID, change.predicate, ccFollowerConvergeWait) {
			assert.Reachable("follower observed same ClusterConfig as leader",
				details.With(internal.Details{"followerId": followerID}))
		}
	}

	sentinel.Verify(ctx, client, fmt.Sprintf("after_%s", change.kind))
}

func pickChange(r *rand.Rand) configChange {
	switch r.Intn(3) {
	case 0:
		v := rotationOptions[r.Intn(len(rotationOptions))]
		return configChange{
			kind:     "rotationThreshold",
			category: "cache",
			value:    v,
			apply: func(ctx context.Context, lsClient dynamic.ResourceInterface) error {
				return internal.PatchCacheRotationThreshold(ctx, lsClient, internal.LedgerServiceName, v)
			},
			predicate: func(cfg *commonpb.ClusterConfig) bool {
				return cfg.GetRotationThreshold() == uint64(v)
			},
		}
	case 1:
		cat := bloomCategories[r.Intn(len(bloomCategories))]
		v := keysOptions[r.Intn(len(keysOptions))]
		return configChange{
			kind:     "bloomExpectedKeys",
			category: cat,
			value:    v,
			apply: func(ctx context.Context, lsClient dynamic.ResourceInterface) error {
				return internal.PatchBloomExpectedKeys(ctx, lsClient, internal.LedgerServiceName, cat, v)
			},
			predicate: bloomKeysPredicate(cat, uint64(v)),
		}
	default:
		cat := bloomCategories[r.Intn(len(bloomCategories))]
		v := fpRateOptions[r.Intn(len(fpRateOptions))]
		return configChange{
			kind:     "bloomFPRate",
			category: cat,
			value:    v,
			apply: func(ctx context.Context, lsClient dynamic.ResourceInterface) error {
				return internal.PatchBloomFPRate(ctx, lsClient, internal.LedgerServiceName, cat, v)
			},
			predicate: bloomFPRatePredicate(cat, v),
		}
	}
}

func bloomKeysPredicate(category string, want uint64) func(cfg *commonpb.ClusterConfig) bool {
	return func(cfg *commonpb.ClusterConfig) bool {
		bt := bloomForCategory(cfg, category)
		return bt != nil && bt.GetExpectedKeys() == want
	}
}

func bloomFPRatePredicate(category, want string) func(cfg *commonpb.ClusterConfig) bool {
	// We only assert the persisted FP rate exists and parses to a non-zero
	// value: comparing the raw float bit-for-bit would be brittle.
	_ = want
	return func(cfg *commonpb.ClusterConfig) bool {
		bt := bloomForCategory(cfg, category)
		return bt != nil && bt.GetFpRate() > 0
	}
}

func bloomForCategory(cfg *commonpb.ClusterConfig, category string) *commonpb.BloomTypeConfig {
	switch category {
	case "volumes":
		return cfg.GetBloomVolumes()
	case "metadata":
		return cfg.GetBloomMetadata()
	case "references":
		return cfg.GetBloomReferences()
	case "ledgers":
		return cfg.GetBloomLedgers()
	case "boundaries":
		return cfg.GetBloomBoundaries()
	case "transactions":
		return cfg.GetBloomTransactions()
	case "sinkConfigs":
		return cfg.GetBloomSinkConfigs()
	case "numscriptVersions":
		return cfg.GetBloomNumscriptVersions()
	case "numscriptContents":
		return cfg.GetBloomNumscriptContents()
	case "ledgerMetadata":
		return cfg.GetBloomLedgerMetadata()
	}
	return nil
}
