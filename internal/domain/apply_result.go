package domain

import "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

// ApplyResult preserves the execution provenance of a committed batch.
// Replayed logs describe a historical outcome, not the current resource state.
type ApplyResult struct {
	Logs     []*ledgerpb.Log
	Replayed bool
}
