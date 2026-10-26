package main

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"hegel.dev/go/hegel"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func TestGeneratedProbeMaterializesUserAccount(t *testing.T) {
	t.Parallel()
	hegel.Test(t, func(tc *hegel.T) {
		dest, amount := generatedProbe(generation.NewSource(tc))
		require.True(tc, strings.HasPrefix(dest, "users:"))
		id, err := strconv.ParseUint(strings.TrimPrefix(dest, "users:"), 10, 64)
		require.NoError(tc, err)
		require.Less(tc, id, internal.UserAccountCount)
		require.Positive(tc, amount.Sign(), "zero must not make the read-back witness disappear")
		require.True(tc, amount.IsUint64())
	}, hegel.WithProfile("base"), hegel.WithBackend(hegel.BackendDefault), hegel.WithDatabase(""), hegel.WithSeed(42), hegel.WithTestCases(32), hegel.WithPhases(hegel.PhaseGenerate))
}
