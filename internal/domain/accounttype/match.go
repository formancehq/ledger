package accounttype

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// FindMatchingType finds the best matching account type for an address using
// longest-match (highest specificity). Returns nil if no type matches.
// Deprecated types are skipped.
func FindMatchingType(
	address string,
	types map[string]*commonpb.AccountType,
) *commonpb.AccountType {
	var (
		best     *commonpb.AccountType
		bestSpec = -1
		bestLen  = 0
	)

	for _, at := range types {
		if at.GetStatus() == commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED {
			continue
		}

		segments, err := ParsePattern(at.GetPattern())
		if err != nil {
			continue
		}

		if _, ok := MatchAddress(address, segments); !ok {
			continue
		}

		spec := Specificity(segments)
		segLen := len(segments)

		if spec > bestSpec || (spec == bestSpec && segLen < bestLen) {
			best = at
			bestSpec = spec
			bestLen = segLen
		}
	}

	return best
}
