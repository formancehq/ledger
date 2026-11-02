package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"hegel.dev/go/hegel"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func TestGeneratedConfigChangeLegalMenus(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	hegel.Test(t, func(tc *hegel.T) {
		change := pickChange(generation.NewSource(tc))
		seen[change.kind] = true
		require.NotNil(tc, change.apply)
		switch change.kind {
		case "rotationThreshold":
			require.Equal(tc, "cache", change.category)
			require.Contains(tc, rotationOptions, change.value)
			cfg := &commonpb.ClusterConfig{RotationThreshold: uint64(change.value.(int32))}
			require.True(tc, change.predicate(cfg))
			cfg.RotationThreshold++
			require.False(tc, change.predicate(cfg))
		case "bloomExpectedKeys", "bloomFPRate":
			require.Contains(tc, bloomCategories, change.category)
			bloom := &commonpb.BloomTypeConfig{}
			cfg := &commonpb.ClusterConfig{
				BloomVolumes: bloom, BloomMetadata: bloom, BloomReferences: bloom,
				BloomLedgers: bloom, BloomTransactions: bloom,
			}
			require.False(tc, change.predicate(cfg))
			if change.kind == "bloomExpectedKeys" {
				require.Contains(tc, keysOptions, change.value)
				bloom.ExpectedKeys = uint64(change.value.(int64))
			} else {
				require.Contains(tc, fpRateOptions, change.value)
				// Retain the existing oracle: any positive persisted FP rate.
				bloom.FpRate = 0.01
			}
			require.True(tc, change.predicate(cfg))
		default:
			tc.Fatalf("unexpected change kind %q", change.kind)
		}
	}, hegel.WithProfile("base"), hegel.WithBackend(hegel.BackendDefault), hegel.WithDatabase(""), hegel.WithSeed(42), hegel.WithTestCases(100), hegel.WithPhases(hegel.PhaseGenerate))
	for _, kind := range []string{"rotationThreshold", "bloomExpectedKeys", "bloomFPRate"} {
		require.True(t, seen[kind], "seeded run must exercise %s", kind)
	}
}
