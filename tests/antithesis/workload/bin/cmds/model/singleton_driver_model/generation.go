package main

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

// runGeneration owns one bounded episode. Expected SUT rejections remain normal
// observations for the model; only a generation-engine failure stops the driver.
// RunOnce prevents engine failure replay from repeating RPC side effects.
func runGeneration(fn func(*generation.Source)) {
	if err := generation.RunOnce(fn); err != nil {
		assert.Unreachable("singleton_driver_model: generation engine failed", internal.Details{"error": err.Error()})
		panic(fmt.Errorf("singleton_driver_model generation: %w", err))
	}
}

// Technical identity generation intentionally retains internal.Rand in run IDs,
// idempotencyKey, txRef and absentLedgerName. These full-width unique names are
// not business choices. Every bounded business draw uses the episode's Source.
