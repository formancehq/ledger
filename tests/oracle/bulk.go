package oracle

import "github.com/formancehq/ledger/v3/internal/proto/servicepb"

// Bulk is one Apply call's worth of requests — the unit the model folds in one
// atomic step, mirroring the server's per-ApplyBatch atomicity.
//
// IdempotencyKey is the batch's idempotency key (idempotency is per ApplyBatch,
// not per request). When empty the model applies the bulk with no idempotency
// bookkeeping. When set, Apply freezes the committed outcome under the key and
// replays it verbatim for any later bulk carrying the same key (same body), or
// rejects a same-key/different-body bulk as a conflict — see GlobalState.Apply.
type Bulk struct {
	Requests       []*servicepb.Request
	IdempotencyKey string
}
