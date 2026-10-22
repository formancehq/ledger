package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"hegel.dev/go/hegel"

	"github.com/formancehq/invariants"
	"github.com/formancehq/ledger/v3/internal/domain"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

// Collect only values, never a callback-scoped Source. Both the pure property
// and the real-server test use the production selector with a reproducible seed.
func projectionProbes(t *testing.T) map[string]uint32 {
	t.Helper()
	probes := make(map[string]uint32)
	hegel.Test(t, func(tc *hegel.T) {
		base, precision := projectionProbe(generation.NewSource(tc))
		require.Contains(tc, []string{"USD", "EUR", "COIN"}, base)
		require.LessOrEqual(tc, precision, uint32(domain.MaxHasAssetPrecision))
		asset := invariants.FormatAsset(base, uint8(precision))
		require.NoError(tc, domain.ValidateAsset(asset))
		actualBase, actualPrecision := invariants.ParseAssetPrecision(asset)
		require.Equal(tc, base, actualBase)
		require.Equal(tc, precision, uint32(actualPrecision))
		probes[fmt.Sprintf("%s/%d", base, precision)] = precision
	}, hegel.WithProfile("base"), hegel.WithBackend(hegel.BackendDefault),
		hegel.WithDatabase(""), hegel.WithSeed(73), hegel.WithTestCases(100),
		hegel.WithPhases(hegel.PhaseGenerate))
	require.Contains(t, probes, "USD/2", "retain the fixed regression branch")
	require.Greater(t, len(probes), 1, "generate business witnesses beyond USD/2")

	return probes
}

func TestProjectionProbeGeneration(t *testing.T) {
	t.Parallel()
	projectionProbes(t)
}

func TestHegelDriverBusinessAssertions(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	drivertest.CheckDriver(t, main,
		"projection-aligned prepared query succeeded",
		"projection-aligned read includes the acknowledged write")
}

func TestProjectionFixedAndGeneratedWitnesses(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	// The SDK emits only the first hit per assertion polarity. Use a fresh
	// subprocess for every variant so every alignment oracle must be observed.
	for _, variant := range []string{"fixed", "zero precision", "maximum precision", "generated"} {
		t.Run(variant, func(t *testing.T) {
			drivertest.CheckEmissions(t, func() {
				base, precision := "USD", uint32(2)
				switch variant {
				case "zero precision":
					base, precision = "EUR", 0
				case "maximum precision":
					base, precision = "COIN", domain.MaxHasAssetPrecision
				case "generated":
					for asset, candidate := range projectionProbes(t) {
						if asset != "USD/2" {
							// The map keys describe base/precision, including zero precision.
							candidateBase, _, ok := strings.Cut(asset, "/")
							require.True(t, ok)
							base, precision = candidateBase, candidate

							break
						}
					}
					require.NotEqual(t, "USD/2", fmt.Sprintf("%s/%d", base, precision))
				}
				ctx, client := drivertest.StartServer(t)
				runProjectionAlignment(ctx, client, base, precision)
			}, func(records []drivertest.Assertion) {
				found := false
				for _, record := range records {
					if !record.Hit {
						continue
					}
					if record.Type == "always" || record.Type == "reachability" {
						require.True(t, record.Condition, "unexpected invariant failure: %+v", record)
					}
					if record.Message != "projection-aligned read includes the acknowledged write" {
						continue
					}
					found = true
					require.True(t, record.Condition)
					require.Greater(t, record.Details["ackedSeq"].(float64), float64(0))
					base, precision := record.Details["assetBase"], record.Details["precision"]
					switch variant {
					case "fixed":
						require.Equal(t, "USD", base)
						require.Equal(t, float64(2), precision)
					case "zero precision":
						require.Equal(t, "EUR", base)
						require.Equal(t, float64(0), precision)
					case "maximum precision":
						require.Equal(t, "COIN", base)
						require.Equal(t, float64(domain.MaxHasAssetPrecision), precision)
					case "generated":
						require.NotEqual(t, "USD/2", fmt.Sprintf("%s/%.0f", base, precision))
					}
				}
				require.True(t, found, "alignment assertion must be evaluated for %s", variant)
			})
		})
	}
}
