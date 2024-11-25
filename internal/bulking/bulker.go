package bulking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/v2/logging"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"sync/atomic"
)

type Bulker struct {
	ctrl        ledgercontroller.Controller
	parallelism int
}

func (b *Bulker) run(ctx context.Context, ctrl ledgercontroller.Controller, bulk Bulk, result chan BulkElementResult, continueOnFailure, parallel bool) bool {

	parallelism := 1
	if parallel && b.parallelism != 0 {
		parallelism = b.parallelism
	}

	wp := pond.New(parallelism, parallelism)
	hasError := atomic.Bool{}

	for element := range bulk {
		wp.Submit(func() {
			select {
			case <-ctx.Done():
				result <- BulkElementResult{
					Error: ctx.Err(),
				}
			default:
				if hasError.Load() && !continueOnFailure {
					result <- BulkElementResult{
						Error: context.Canceled,
					}
					return
				}
				ret, logID, err := b.processElement(ctx, ctrl, element)
				if err != nil {
					hasError.Store(true)

					result <- BulkElementResult{
						Error: err,
					}

					return
				}

				result <- BulkElementResult{
					Data:  ret,
					LogID: logID,
				}
			}

		})
	}

	wp.StopAndWait()

	close(result)

	return hasError.Load()
}

func (b *Bulker) Run(ctx context.Context, bulk Bulk, result chan BulkElementResult, bulkOptions BulkingOptions) error {

	if err := bulkOptions.Validate(); err != nil {
		return fmt.Errorf("validating bulk options: %s", err)
	}

	ctrl := b.ctrl
	if bulkOptions.Atomic {
		var err error
		ctrl, err = ctrl.BeginTX(ctx, nil)
		if err != nil {
			return fmt.Errorf("error starting transaction: %s", err)
		}
	}

	hasError := b.run(ctx, ctrl, bulk, result, bulkOptions.ContinueOnFailure, bulkOptions.Parallel)
	if hasError && bulkOptions.Atomic {
		if rollbackErr := ctrl.Rollback(ctx); rollbackErr != nil {
			logging.FromContext(ctx).Errorf("failed to rollback transaction: %v", rollbackErr)
		}

		return nil
	}

	if bulkOptions.Atomic {
		if err := ctrl.Commit(ctx); err != nil {
			return fmt.Errorf("error committing transaction: %s", err)
		}
	}

	return nil
}

func (b *Bulker) processElement(ctx context.Context, ctrl ledgercontroller.Controller, data BulkElement) (any, int, error) {

	switch data.Action {
	case ActionCreateTransaction:
		rs, err := data.Data.(TransactionRequest).ToRunScript(false)
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

		log, createTransactionResult, err := ctrl.CreateTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.RunScript]{
			DryRun:         false,
			IdempotencyKey: data.IdempotencyKey,
			Input:          *rs,
		})
		if err != nil {
			return nil, 0, err
		}

		return createTransactionResult.Transaction, log.ID, nil
	case ActionAddMetadata:
		req := data.Data.(AddMetadataRequest)

		var (
			log *ledger.Log
			err error
		)
		switch req.TargetType {
		case ledger.MetaTargetTypeAccount:
			address := ""
			if err := json.Unmarshal(req.TargetID, &address); err != nil {
				return nil, 0, err
			}
			log, err = ctrl.SaveAccountMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: ledgercontroller.SaveAccountMetadata{
					Address:  address,
					Metadata: req.Metadata,
				},
			})
		case ledger.MetaTargetTypeTransaction:
			transactionID := 0
			if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
				return nil, 0, err
			}
			log, err = ctrl.SaveTransactionMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: ledgercontroller.SaveTransactionMetadata{
					TransactionID: transactionID,
					Metadata:      req.Metadata,
				},
			})
		default:
			return nil, 0, fmt.Errorf("invalid target type: %s", req.TargetType)
		}
		if err != nil {
			return nil, 0, err
		}

		return nil, log.ID, nil
	case ActionRevertTransaction:
		req := data.Data.(RevertTransactionRequest)

		log, revertTransactionResult, err := ctrl.RevertTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{
			DryRun:         false,
			IdempotencyKey: data.IdempotencyKey,
			Input: ledgercontroller.RevertTransaction{
				Force:           req.Force,
				AtEffectiveDate: req.AtEffectiveDate,
				TransactionID:   req.ID,
			},
		})
		if err != nil {
			return nil, 0, err
		}

		return revertTransactionResult.RevertedTransaction, log.ID, nil
	case ActionDeleteMetadata:
		req := data.Data.(DeleteMetadataRequest)

		var (
			log *ledger.Log
			err error
		)
		switch req.TargetType {
		case ledger.MetaTargetTypeAccount:
			address := ""
			if err := json.Unmarshal(req.TargetID, &address); err != nil {
				return nil, 0, err
			}

			log, err = ctrl.DeleteAccountMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.DeleteAccountMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: ledgercontroller.DeleteAccountMetadata{
					Address: address,
					Key:     req.Key,
				},
			})
		case ledger.MetaTargetTypeTransaction:
			transactionID := 0
			if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
				return nil, 0, err
			}

			log, err = ctrl.DeleteTransactionMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: ledgercontroller.DeleteTransactionMetadata{
					TransactionID: transactionID,
					Key:           req.Key,
				},
			})
		default:
			return nil, 0, fmt.Errorf("unsupported target type: %s", req.TargetType)
		}
		if err != nil {
			return nil, 0, err
		}

		return nil, log.ID, nil
	default:
		panic("unreachable")
	}
}

func NewBulker(ctrl ledgercontroller.Controller, options ...BulkerOption) *Bulker {
	ret := &Bulker{ctrl: ctrl}
	for _, option := range options {
		option(ret)
	}

	return ret
}

type BulkerOption func(bulker *Bulker)

func WithParallelism(v int) BulkerOption {
	return func(options *Bulker) {
		options.parallelism = v
	}
}

type BulkingOptions struct {
	ContinueOnFailure bool
	Atomic            bool
	Parallel          bool
}

func (opts BulkingOptions) Validate() error {
	if opts.Atomic && opts.Parallel {
		return errors.New("atomic and parallel options are mutually exclusive")
	}

	return nil
}

type BulkerFactory interface {
	CreateBulker(ctrl ledgercontroller.Controller) *Bulker
}

type DefaultBulkerFactory struct {
	Options []BulkerOption
}

func (d *DefaultBulkerFactory) CreateBulker(ctrl ledgercontroller.Controller) *Bulker {
	return NewBulker(ctrl, d.Options...)
}

func NewDefaultBulkerFactory(options ...BulkerOption) *DefaultBulkerFactory {
	return &DefaultBulkerFactory{
		Options: options,
	}
}

var _ BulkerFactory = (*DefaultBulkerFactory)(nil)

