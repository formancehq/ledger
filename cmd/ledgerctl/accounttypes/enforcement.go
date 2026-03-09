package accounttypes

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// parseEnforcementMode converts a string to a ChartEnforcementMode proto enum.
func parseEnforcementMode(s string) (commonpb.ChartEnforcementMode, error) {
	switch strings.ToUpper(s) {
	case "STRICT":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, nil
	case "AUDIT":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, nil
	default:
		return 0, fmt.Errorf("invalid enforcement mode %q: must be STRICT or AUDIT", s)
	}
}
