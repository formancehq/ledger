package http

import (
	"errors"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

// parseEnforcementMode converts a string to a ChartEnforcementMode proto enum.
func parseEnforcementMode(s string) (ledgerpb.ChartEnforcementMode, error) {
	switch s {
	case "STRICT", "strict":
		return ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, nil
	case "AUDIT", "audit":
		return ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, nil
	default:
		return 0, errors.New("invalid enforcement mode: must be STRICT or AUDIT")
	}
}
