package ctrl

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ledgerInfoCursor enriches mirrors through the listing's existing snapshot.
// It performs three point reads per consumed mirror, with no new snapshot or
// persisted projection. Close delegates to the cursor that owns the handle.
type ledgerInfoCursor struct {
	cursor.Cursor[*commonpb.LedgerInfo]

	ctx    context.Context
	handle *dal.ReadHandle
	attrs  *attributes.Attributes
}

func (c *ledgerInfoCursor) Next() (*commonpb.LedgerInfo, error) {
	ledger, err := c.Cursor.Next()
	if err != nil {
		return nil, err
	}
	if ledger.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		progress, err := query.ReadMirrorSyncProgress(c.ctx, c.handle, c.attrs.Boundary, ledger.GetName())
		if err != nil {
			return nil, fmt.Errorf("reading listed ledger mirror progress: %w", err)
		}
		ledger.MirrorSyncProgress = progress
	}

	return ledger, nil
}
