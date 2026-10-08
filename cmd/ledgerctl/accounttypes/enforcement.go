package accounttypes

import (
	"fmt"
	"strings"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// parseEnforcementMode converts a string to a ChartEnforcementMode proto enum.
func parseEnforcementMode(s string) (ledgerpb.ChartEnforcementMode, error) {
	switch strings.ToUpper(s) {
	case "STRICT":
		return ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, nil
	case "AUDIT":
		return ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, nil
	default:
		return 0, fmt.Errorf("invalid enforcement mode %q: must be STRICT or AUDIT", s)
	}
}
