package main

import (
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

func TestHegelDriverBusinessAssertions(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	drivertest.CheckDriver(t, main,
		"duplicate reference should return TRANSACTION_REFERENCE_CONFLICT",
		"reference conflict path exercised",
	)
}
