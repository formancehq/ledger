package main

import (
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

// Exercise the generated driver against a real server and require business
// assertion hits, not just catalog registration or a successful process exit.
func TestHegelDriverBusinessAssertions(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	drivertest.CheckDriver(t, main,
		"mismatched idempotency key should return IDEMPOTENCY_KEY_CONFLICT",
		"idempotency conflict path exercised",
	)
}
