package domain

import commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

// ApplyResult preserves the execution provenance of a committed batch.
// Replayed logs describe a historical outcome, not the current resource state.
type ApplyResult struct {
	Logs     []*commonpb.Log
	Replayed bool
}
