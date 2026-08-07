package main

import (
	"slices"
	"testing"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func TestDifferentScalingTargetNeverReturnsCurrentReplicaCount(t *testing.T) {
	t.Parallel()

	for _, current := range internal.OddReplicas {
		for start := range len(internal.OddReplicas) {
			target := differentScalingTarget(current, start)
			if target == current {
				t.Fatalf("current=%d start=%d returned the current replica count", current, start)
			}
			if !slices.Contains(internal.OddReplicas, target) {
				t.Fatalf("current=%d start=%d returned unsupported target %d", current, start, target)
			}
		}
	}
}
