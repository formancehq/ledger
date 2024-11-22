package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
)

const (
	ActionCreateTransaction = "CREATE_TRANSACTION"
	ActionAddMetadata       = "ADD_METADATA"
	ActionRevertTransaction = "REVERT_TRANSACTION"
	ActionDeleteMetadata    = "DELETE_METADATA"
)

type Bulk []BulkElement

type BulkResult []BulkElementResult

func (r BulkResult) HasErrors() bool {
	for _, element := range r {
		if element.Error != nil {
			return true
		}
	}

	return false
}

type BulkElement struct {
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
	ctrl Controller
}

func (b *Bulker) run(ctx context.Context, ctrl Controller, bulk Bulk, continueOnFailure bool) (BulkResult, error) {

	results := make([]BulkElementResult, 0, len(bulk))

	for _, element := range bulk {
		ret, logID, err := b.processElement(ctx, ctrl, element)
		if err != nil {
			results = append(results, BulkElementResult{
				Error: err,
			})

			if !continueOnFailure {
				return results, nil
			}

			continue
		}

		results = append(results, BulkElementResult{
			Data:  ret,
			LogID: logID,
		})
	}

	return results, nil
}

func (b *Bulker) Run(ctx context.Context, bulk Bulk, continueOnFailure, atomic bool) (BulkResult, error) {

	for i, element := range bulk {
		switch element.Action {
		case ActionCreateTransaction:
			req := &TransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, fmt.Errorf("error parsing element %d: %s", i, err)
			}
		case ActionAddMetadata:
			req := &AddMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, fmt.Errorf("error parsing element %d: %s", i, err)
			}
		case ActionRevertTransaction:
			req := &RevertTransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, fmt.Errorf("error parsing element %d: %s", i, err)
			}
		case ActionDeleteMetadata:
			req := &DeleteMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, fmt.Errorf("error parsing element %d: %s", i, err)
			}
		}
	}

	ctrl := b.ctrl
	if atomic {
		var err error
		ctrl, err = ctrl.BeginTX(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("error starting transaction: %s", err)
		}
	}

	results, err := b.run(ctx, ctrl, bulk, continueOnFailure)
	if err != nil {
		if atomic {
			if rollbackErr := ctrl.Rollback(ctx); rollbackErr != nil {
				logging.FromContext(ctx).Errorf("failed to rollback transaction: %v", rollbackErr)
			}
		}

		return nil, fmt.Errorf("error running bulk: %s", err)
	}

	if atomic {
		if results.HasErrors() {
			if rollbackErr := ctrl.Rollback(ctx); rollbackErr != nil {
				logging.FromContext(ctx).Errorf("failed to rollback transaction: %v", rollbackErr)
			}
		} else {
			if err := ctrl.Commit(ctx); err != nil {
				return nil, fmt.Errorf("error committing transaction: %s", err)
			}
		}
	}

	return results, err
}

func (b *Bulker) processElement(ctx context.Context, ctrl Controller, element BulkElement) (any, int, error) {

	switch element.Action {
	case ActionCreateTransaction:
		req := &TransactionRequest{}
		if err := json.Unmarshal(element.Data, req); err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}
		rs, err := req.ToRunScript(false)
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

		log, createTransactionResult, err := ctrl.CreateTransaction(ctx, Parameters[RunScript]{
			DryRun:         false,
			IdempotencyKey: element.IdempotencyKey,
			Input:          *rs,
		})
		if err != nil {
			return nil, 0, err
		}

		return createTransactionResult.Transaction, log.ID, nil
	case ActionAddMetadata:
		req := &AddMetadataRequest{}
		if err := json.Unmarshal(element.Data, req); err != nil {
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
				IdempotencyKey: element.IdempotencyKey,
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
				IdempotencyKey: element.IdempotencyKey,
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
		if err := json.Unmarshal(element.Data, req); err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

		log, revertTransactionResult, err := ctrl.RevertTransaction(ctx, Parameters[RevertTransaction]{
			DryRun:         false,
			IdempotencyKey: element.IdempotencyKey,
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
		if err := json.Unmarshal(element.Data, req); err != nil {
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
				IdempotencyKey: element.IdempotencyKey,
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
				IdempotencyKey: element.IdempotencyKey,
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

func NewBulker(ctrl Controller) *Bulker {
	return &Bulker{ctrl: ctrl}
}
