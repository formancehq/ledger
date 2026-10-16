package http

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// parseLedgerMode converts the create-ledger "mode" field to a LedgerMode proto
// enum. An empty value selects NORMAL; any value outside the documented enum is
// refused so that a mistyped MIRROR never silently creates a writable ledger.
func parseLedgerMode(s string) (commonpb.LedgerMode, error) {
	switch s {
	case "", "NORMAL":
		return commonpb.LedgerMode_LEDGER_MODE_NORMAL, nil
	case "MIRROR":
		return commonpb.LedgerMode_LEDGER_MODE_MIRROR, nil
	default:
		return 0, errors.New("invalid ledger mode: must be NORMAL or MIRROR")
	}
}
