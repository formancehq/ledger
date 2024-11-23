package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"sync/atomic"
)

const (
	ActionCreateTransaction = "CREATE_TRANSACTION"
	ActionAddMetadata       = "ADD_METADATA"
	ActionRevertTransaction = "REVERT_TRANSACTION"
	ActionDeleteMetadata    = "DELETE_METADATA"
)

type Bulk chan BulkElement

type BulkElement struct {
	Data     BulkElementData
	Response chan BulkElementResult
}

type BulkElementData struct {
	Action         string          `json:"action"`
	IdempotencyKey string          `json:"ik"`
	Data           json.RawMessage `json:"data"`
}

type BulkElementResult struct {
	Error error
	Data  any `json:"data,omitempty"`
	LogID int `json:"logID"`
}

type AddMetadataRequest struct {
	TargetType string            `json:"targetType"`
	TargetID   json.RawMessage   `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

type RevertTransactionRequest struct {
	ID              int  `json:"id"`
	Force           bool `json:"force"`
	AtEffectiveDate bool `json:"atEffectiveDate"`
}

type DeleteMetadataRequest struct {
	TargetType string          `json:"targetType"`
	TargetID   json.RawMessage `json:"targetId"`
	Key        string          `json:"key"`
}

type TransactionRequest struct {
	Postings  ledger.Postings   `json:"postings"`
	Script    ScriptV1          `json:"script"`
	Timestamp time.Time         `json:"timestamp"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata" swaggertype:"object"`
}

func (req *TransactionRequest) ToRunScript(allowUnboundedOverdrafts bool) (*RunScript, error) {

	if _, err := req.Postings.Validate(); err != nil {
		return nil, err
	}

	if len(req.Postings) > 0 {
		txData := ledger.TransactionData{
			Postings:  req.Postings,
			Timestamp: req.Timestamp,
			Reference: req.Reference,
			Metadata:  req.Metadata,
		}

		return pointer.For(TxToScriptData(txData, allowUnboundedOverdrafts)), nil
	}

	return &RunScript{
		Script:    req.Script.ToCore(),
		Timestamp: req.Timestamp,
		Reference: req.Reference,
		Metadata:  req.Metadata,
	}, nil
}

type Bulker struct {
	ctrl        Controller
	parallelism int
}

func (b *Bulker) run(ctx context.Context, ctrl Controller, bulk Bulk, continueOnFailure, parallel bool) bool {

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
				element.Response <- BulkElementResult{
					Error: ctx.Err(),
				}
			default:
				if hasError.Load() && !continueOnFailure {
					element.Response <- BulkElementResult{
						Error: context.Canceled,
					}
					return
				}
				ret, logID, err := b.processElement(ctx, ctrl, element.Data)
				if err != nil {
					hasError.Store(true)

					element.Response <- BulkElementResult{
						Error: err,
					}

					return
				}

				element.Response <- BulkElementResult{
					Data:  ret,
					LogID: logID,
				}
			}

		})
	}

	wp.StopAndWait()

	return hasError.Load()
}

func (b *Bulker) Run(ctx context.Context, bulk Bulk, providedOptions ...BulkingOption) error {

	bulkOptions := BulkingOptions{}
	for _, option := range providedOptions {
		option(&bulkOptions)
	}

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

	hasError := b.run(ctx, ctrl, bulk, bulkOptions.ContinueOnFailure, bulkOptions.Parallel)
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

func (b *Bulker) processElement(ctx context.Context, ctrl Controller, data BulkElementData) (any, int, error) {

	switch data.Action {
	case ActionCreateTransaction:
		req := &TransactionRequest{}
		if err := json.Unmarshal(data.Data, req); err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}
		rs, err := req.ToRunScript(false)
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

		log, createTransactionResult, err := ctrl.CreateTransaction(ctx, Parameters[RunScript]{
			DryRun:         false,
			IdempotencyKey: data.IdempotencyKey,
			Input:          *rs,
		})
		if err != nil {
			return nil, 0, err
		}

		return createTransactionResult.Transaction, log.ID, nil
	case ActionAddMetadata:
		req := &AddMetadataRequest{}
		if err := json.Unmarshal(data.Data, req); err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

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
			log, err = ctrl.SaveAccountMetadata(ctx, Parameters[SaveAccountMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: SaveAccountMetadata{
					Address:  address,
					Metadata: req.Metadata,
				},
			})
		case ledger.MetaTargetTypeTransaction:
			transactionID := 0
			if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
				return nil, 0, err
			}
			log, err = ctrl.SaveTransactionMetadata(ctx, Parameters[SaveTransactionMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: SaveTransactionMetadata{
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
		req := &RevertTransactionRequest{}
		if err := json.Unmarshal(data.Data, req); err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

		log, revertTransactionResult, err := ctrl.RevertTransaction(ctx, Parameters[RevertTransaction]{
			DryRun:         false,
			IdempotencyKey: data.IdempotencyKey,
			Input: RevertTransaction{
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
		req := &DeleteMetadataRequest{}
		if err := json.Unmarshal(data.Data, req); err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

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

			log, err = ctrl.DeleteAccountMetadata(ctx, Parameters[DeleteAccountMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: DeleteAccountMetadata{
					Address: address,
					Key:     req.Key,
				},
			})
		case ledger.MetaTargetTypeTransaction:
			transactionID := 0
			if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
				return nil, 0, err
			}

			log, err = ctrl.DeleteTransactionMetadata(ctx, Parameters[DeleteTransactionMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: DeleteTransactionMetadata{
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

func NewBulker(ctrl Controller, options ...BulkerOption) *Bulker {
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

type BulkingOption func(*BulkingOptions)

func WithContinueOnFailure(v bool) BulkingOption {
	return func(options *BulkingOptions) {
		options.ContinueOnFailure = v
	}
}

func WithAtomic(v bool) BulkingOption {
	return func(options *BulkingOptions) {
		options.Atomic = v
	}
}

func WithParallel(v bool) BulkingOption {
	return func(options *BulkingOptions) {
		options.Parallel = v
	}
}

type BulkerFactory interface {
	CreateBulker(ctrl Controller) *Bulker
}

type DefaultBulkerFactory struct {
	Options []BulkerOption
}

func (d *DefaultBulkerFactory) CreateBulker(ctrl Controller) *Bulker {
	return NewBulker(ctrl, d.Options...)
}

func NewDefaultBulkerFactory(options ...BulkerOption) *DefaultBulkerFactory {
	return &DefaultBulkerFactory{
		Options: options,
	}
}

var _ BulkerFactory = (*DefaultBulkerFactory)(nil)
