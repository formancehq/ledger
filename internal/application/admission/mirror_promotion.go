package admission

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// ErrMirrorNotCaughtUp is an admission-only precondition. It is deliberately
// not a new ErrorReason: the rc.1 protobuf and service protocol are frozen.
type ErrMirrorNotCaughtUp struct {
	Ledger    string
	Cursor    uint64
	Head      uint64
	SyncError bool
}

func (e *ErrMirrorNotCaughtUp) Error() string {
	return fmt.Sprintf("mirror %s is not caught up: cursor=%d head=%d sync_error=%t",
		e.Ledger, e.Cursor, e.Head, e.SyncError)
}

func (*ErrMirrorNotCaughtUp) Kind() domain.ErrorKind { return domain.KindPrecondition }

// checkMirrorPromotions uses one read handle for each promotion. An unknown
// source head is unsafe: it also describes a mirror whose worker never reached
// the source. Apply remains unchanged so old and new voters replay committed
// PromoteLedger entries identically during an RC rolling upgrade.
func (a *Admission) checkMirrorPromotions(ctx context.Context, orders []*raftcmdpb.Order, idempotencyKey string) error {
	for _, order := range orders {
		ledgerOrder := order.GetLedgerScoped()
		if ledgerOrder == nil || ledgerOrder.GetPromoteLedger() == nil {
			continue
		}

		handle, err := a.store.NewReadHandle()
		if err != nil {
			return fmt.Errorf("opening mirror promotion read handle: %w", err)
		}
		info, infoErr := query.ReadLedgerInfoRow(handle, ledgerOrder.GetLedger())
		if infoErr != nil {
			_ = handle.Close()

			return fmt.Errorf("reading ledger for mirror promotion: %w", infoErr)
		}
		if info == nil || (createsLedgerInBatch(orders, ledgerOrder.GetLedger()) && info.GetDeletedAt() != nil) {
			// An absent ledger might be created as a mirror before proposal;
			// never let an unobserved head bypass the eligibility gate. A
			// create-then-promote batch has the same problem, even if a
			// previous ledger with this name left a tombstone.
			_ = handle.Close()

			return &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: ledgerOrder.GetLedger()}}
		}
		if info.GetDeletedAt() != nil {
			// Deleted names cannot be recreated. Let the FSM distinguish a
			// fresh rejection from a keyed replay after deletion.
			if err := handle.Close(); err != nil {
				return fmt.Errorf("closing mirror promotion read handle: %w", err)
			}

			continue
		}
		if info.GetMode() != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
			// A committed promotion changes the ledger to NORMAL. Only this
			// post-promotion state can bypass eligibility for a keyed retry.
			// An expired retained key is harmless here: the FSM may decline
			// replay, but cannot freshly promote an already-normal ledger.
			if idempotencyKey != "" {
				value, readErr := state.LoadIdempotencyKey(handle, idempotencyKey)
				if readErr != nil {
					_ = handle.Close()

					return fmt.Errorf("reading mirror promotion idempotency key: %w", readErr)
				}
				if value != nil {
					if err := handle.Close(); err != nil {
						return fmt.Errorf("closing mirror promotion read handle: %w", err)
					}

					continue
				}
			}
			_ = handle.Close()

			return &domain.BusinessError{Err: &domain.ErrLedgerNotInMirrorMode{Name: ledgerOrder.GetLedger()}}
		}
		progress, readErr := query.ReadMirrorSyncProgress(ctx, handle, a.attrs.Boundary, ledgerOrder.GetLedger())
		closeErr := handle.Close()
		if readErr != nil {
			return fmt.Errorf("reading mirror promotion progress: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("closing mirror promotion read handle: %w", closeErr)
		}
		if progress.GetSourceLogCount() == 0 || progress.GetCursor() < progress.GetSourceLogCount() || progress.GetError() != nil {
			return &ErrMirrorNotCaughtUp{
				Ledger: ledgerOrder.GetLedger(), Cursor: progress.GetCursor(),
				Head: progress.GetSourceLogCount(), SyncError: progress.GetError() != nil,
			}
		}
	}

	return nil
}

func createsLedgerInBatch(orders []*raftcmdpb.Order, name string) bool {
	for _, order := range orders {
		ledgerOrder := order.GetLedgerScoped()
		if ledgerOrder != nil && ledgerOrder.GetLedger() == name && ledgerOrder.GetCreateLedger() != nil {
			return true
		}
	}

	return false
}
