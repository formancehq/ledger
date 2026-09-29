package http

import (
	"errors"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// parseEnforcementMode converts a string to a ChartEnforcementMode proto enum.
func parseEnforcementMode(s string) (commonpb.ChartEnforcementMode, error) {
	switch s {
	case "STRICT", "strict":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, nil
	case "AUDIT", "audit":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, nil
	default:
		return 0, errors.New("invalid enforcement mode: must be STRICT or AUDIT")
	}
}
