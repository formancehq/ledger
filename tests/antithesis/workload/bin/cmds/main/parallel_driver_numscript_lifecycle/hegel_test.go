package main

import (
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"hegel.dev/go/hegel"

	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func lifecycleVersionPairs(t *testing.T) map[string][2]string {
	t.Helper()
	pairs := make(map[string][2]string)
	hegel.Test(t, func(tc *hegel.T) {
		first, later := numscriptVersions(generation.NewSource(tc))
		v1, err := semver.Parse(first)
		require.NoError(tc, err)
		v2, err := semver.Parse(later)
		require.NoError(tc, err)
		require.Equal(tc, -1, v1.Compare(v2), "second save must be the greatest semver")
		if first == "1.0.0" && later == "2.0.0" {
			pairs["fixed"] = [2]string{first, later}
		} else {
			switch {
			case v2.Major > v1.Major:
				pairs["major"] = [2]string{first, later}
			case v2.Minor > v1.Minor:
				pairs["minor"] = [2]string{first, later}
			default:
				pairs["patch"] = [2]string{first, later}
			}
		}
	}, hegel.WithProfile("base"), hegel.WithBackend(hegel.BackendDefault),
		hegel.WithDatabase(""), hegel.WithSeed(93), hegel.WithTestCases(100),
		hegel.WithPhases(hegel.PhaseGenerate))
	for _, branch := range []string{"fixed", "major", "minor", "patch"} {
		require.Contains(t, pairs, branch)
	}

	return pairs
}

func TestNumscriptVersionGeneration(t *testing.T) {
	t.Parallel()
	lifecycleVersionPairs(t)
}

func TestHegelDriverBusinessAssertions(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	drivertest.CheckDriver(t, main,
		"saved numscript should appear in ListNumscripts",
		"GetNumscript should return latest version",
		"latest pointer should be the greatest saved semver")
}

func TestNumscriptFixedAndGeneratedVersions(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	// A separate process per pair avoids SDK first-hit deduplication hiding
	// later variants. All three business oracles must fire for every branch.
	for _, branch := range []string{"fixed", "major", "minor", "patch"} {
		t.Run(branch, func(t *testing.T) {
			drivertest.CheckEmissions(t, func() {
				pair := lifecycleVersionPairs(t)[branch]
				ctx, client := drivertest.StartServer(t)
				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("seed", actions.CreateLedgerAction("default", nil)))
				require.NoError(t, err)
				runNumscriptLifecycle(ctx, client, "default", pair[0], pair[1])
			}, func(records []drivertest.Assertion) {
				seen := make(map[string]bool)
				for _, record := range records {
					if !record.Hit {
						continue
					}
					if record.Type == "always" || record.Type == "reachability" {
						require.True(t, record.Condition, "unexpected invariant failure: %+v", record)
					}
					switch record.Message {
					case "saved numscript should appear in ListNumscripts",
						"GetNumscript should return latest version",
						"latest pointer should be the greatest saved semver":
						require.True(t, record.Condition, "oracle failed: %+v", record)
						seen[record.Message] = true
						first, err := semver.Parse(record.Details["firstVersion"].(string))
						require.NoError(t, err)
						later, err := semver.Parse(record.Details["laterVersion"].(string))
						require.NoError(t, err)
						require.Equal(t, -1, first.Compare(later))
						switch branch {
						case "fixed":
							require.Equal(t, "1.0.0", first.String())
							require.Equal(t, "2.0.0", later.String())
						case "major":
							require.Greater(t, later.Major, first.Major)
						case "minor":
							require.Equal(t, first.Major, later.Major)
							require.Greater(t, later.Minor, first.Minor)
						case "patch":
							require.Equal(t, first.Major, later.Major)
							require.Equal(t, first.Minor, later.Minor)
							require.Greater(t, later.Patch, first.Patch)
						}
						if record.Message != "saved numscript should appear in ListNumscripts" {
							require.Equal(t, later.String(), record.Details["expected"])
							require.Equal(t, later.String(), record.Details["actual"])
						}
					}
				}
				require.Len(t, seen, 3, "all lifecycle assertions must be evaluated for %s", branch)
			})
		})
	}
}
