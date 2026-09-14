package domain

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// ApplyResult preserves the execution provenance of a committed batch.
// Replayed logs describe a historical outcome, not the current resource state.
type ApplyResult struct {
	Logs     []*commonpb.Log
	Replayed bool
}
