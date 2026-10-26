package internal_test

import (
	"context"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func TestGeneratedDriverReportsFailureWithoutReplay(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("seed", actions.CreateLedgerAction("default", nil)))
		require.NoError(t, err)
		calls := 0
		internal.RunGeneratedDriver("deliberate-generation-failure", func(_ context.Context, _ servicepb.BucketServiceClient, _ string, src *generation.Source) {
			calls++
			_ = src.Intn(2)
			panic("deliberate generation failure")
		})
		require.Equal(t, 1, calls, "driver effects must not replay after panic")
	}, func(records []drivertest.Assertion) {
		for _, record := range records {
			if record.Message == "Hegel driver generation failed" && record.Hit {
				require.False(t, record.Condition)
				require.Equal(t, "deliberate-generation-failure", record.Details["driver"])

				return
			}
		}
		t.Fatal("generation failure was not emitted to Antithesis")
	})
}

func TestOperationalGenerationReportsFailureWithoutReplay(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	drivertest.CheckEmissions(t, func() {
		calls := 0
		err := internal.RunOperationalGeneration("deliberate-operational-failure", func(src *generation.Source) {
			calls++
			_ = src.Bool()
			panic("deliberate operational failure")
		})
		require.ErrorContains(t, err, "deliberate operational failure")
		require.Equal(t, 1, calls, "operational effects must not replay after panic")
	}, func(records []drivertest.Assertion) {
		for _, record := range records {
			if record.Message == "Hegel operational generation failed" && record.Hit {
				require.False(t, record.Condition)
				require.Equal(t, "deliberate-operational-failure", record.Details["operation"])

				return
			}
		}
		t.Fatal("operational generation failure was not emitted to Antithesis")
	})
}
