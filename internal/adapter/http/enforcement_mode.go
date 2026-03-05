package http

import (
	"errors"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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

// enforcementModeToString converts a ChartEnforcementMode to its string representation.
func enforcementModeToString(mode commonpb.ChartEnforcementMode) string {
	switch mode {
	case commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT:
		return "STRICT"
	case commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT:
		return "AUDIT"
	default:
		return "UNKNOWN"
	}
}
