package oracle

import "github.com/formancehq/ledger/v3/internal/proto/servicepb"

// Bulk is one Apply call's worth of requests — the unit the model folds in one
// atomic step, mirroring the server's per-ApplyBatch atomicity.
type Bulk struct {
	Requests []*servicepb.Request
}
