package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func bulkHandler(bulkMaxSize int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b := Bulk{}
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		if bulkMaxSize != 0 && len(b) > bulkMaxSize {
			api.WriteErrorResponse(w, http.StatusRequestEntityTooLarge, ErrBulkSizeExceeded, fmt.Errorf("bulk size exceeded, max size is %d", bulkMaxSize))
			return
		}

		w.Header().Set("Content-Type", "application/json")

		ret, errorsInBulk, err := ProcessBulk(r.Context(), common.LedgerFromContext(r.Context()), b, api.QueryParamBool(r, "continueOnFailure"))
		if err != nil || errorsInBulk {
			w.WriteHeader(http.StatusBadRequest)
		}

		if err := json.NewEncoder(w).Encode(api.BaseResponse[[]Result]{
			Data: &ret,
		}); err != nil {
			panic(err)
		}
	}
}

const (
	ActionCreateTransaction = "CREATE_TRANSACTION"
	ActionAddMetadata       = "ADD_METADATA"
	ActionRevertTransaction = "REVERT_TRANSACTION"
	ActionDeleteMetadata    = "DELETE_METADATA"
)

type Bulk []BulkElement

type BulkElement struct {
	Action         string          `json:"action"`
	IdempotencyKey string          `json:"ik"`
	Data           json.RawMessage `json:"data"`
}

type Result struct {
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorDescription string `json:"errorDescription,omitempty"`
	ErrorDetails     string `json:"errorDetails,omitempty"`
	Data             any    `json:"data,omitempty"`
	ResponseType     string `json:"responseType"` // Added for sdk generation (discriminator in oneOf)
	LogID            int    `json:"logID"`
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
	Postings  ledger.Postings           `json:"postings"`
	Script    ledgercontroller.ScriptV1 `json:"script"`
	Timestamp time.Time                 `json:"timestamp"`
	Reference string                    `json:"reference"`
	Metadata  metadata.Metadata         `json:"metadata" swaggertype:"object"`
}

func (req *TransactionRequest) ToRunScript(allowUnboundedOverdrafts bool) (*ledgercontroller.RunScript, error) {

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

		return pointer.For(common.TxToScriptData(txData, allowUnboundedOverdrafts)), nil
	}

	return &ledgercontroller.RunScript{
		Script:    req.Script.ToCore(),
		Timestamp: req.Timestamp,
		Reference: req.Reference,
		Metadata:  req.Metadata,
	}, nil
}

func ProcessBulk(
	ctx context.Context,
	l ledgercontroller.Controller,
	bulk Bulk,
	continueOnFailure bool,
) ([]Result, bool, error) {

	ret := make([]Result, 0, len(bulk))

	errorsInBulk := false
	var bulkError = func(action, code string, err error) {
		ret = append(ret, Result{
			ErrorCode:        code,
			ErrorDescription: err.Error(),
			ResponseType:     "ERROR",
		})
		errorsInBulk = true
	}

	for i, element := range bulk {
		switch element.Action {
		case ActionCreateTransaction:
			req := &TransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}
			rs, err := req.ToRunScript(false)
			if err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			log, createTransactionResult, err := l.CreateTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.RunScript]{
				DryRun:         false,
				IdempotencyKey: element.IdempotencyKey,
				Input:          *rs,
			})
			if err != nil {
				var code string

				switch {
				case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}):
					code = ErrInsufficientFund
				case errors.Is(err, &ledgercontroller.ErrInvalidVars{}) || errors.Is(err, ledgercontroller.ErrCompilationFailed{}):
					code = ErrCompilationFailed
				case errors.Is(err, &ledgercontroller.ErrMetadataOverride{}):
					code = ErrMetadataOverride
				case errors.Is(err, ledgercontroller.ErrNoPostings):
					code = ErrNoPostings
				case errors.Is(err, ledgercontroller.ErrTransactionReferenceConflict{}):
					code = ErrConflict
				case errors.Is(err, ledgercontroller.ErrParsing{}):
					code = ErrInterpreterParse
				case errors.Is(err, ledgercontroller.ErrRuntime{}):
					code = ErrInterpreterRuntime
				default:
					code = api.ErrorInternal
				}

				bulkError(element.Action, code, err)
				if !continueOnFailure {
					return ret, errorsInBulk, nil
				}
			} else {
				ret = append(ret, Result{
					Data:         createTransactionResult.Transaction,
					ResponseType: element.Action,
					LogID:        log.ID,
				})
			}
		case ActionAddMetadata:
			req := &AddMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			var (
				log *ledger.Log
				err error
			)
			switch req.TargetType {
			case ledger.MetaTargetTypeAccount:
				address := ""
				if err := json.Unmarshal(req.TargetID, &address); err != nil {
					return nil, errorsInBulk, err
				}
				log, err = l.SaveAccountMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
					DryRun:         false,
					IdempotencyKey: element.IdempotencyKey,
					Input: ledgercontroller.SaveAccountMetadata{
						Address:  address,
						Metadata: req.Metadata,
					},
				})
			case ledger.MetaTargetTypeTransaction:
				transactionID := 0
				if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
					return nil, errorsInBulk, err
				}
				log, err = l.SaveTransactionMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]{
					DryRun:         false,
					IdempotencyKey: element.IdempotencyKey,
					Input: ledgercontroller.SaveTransactionMetadata{
						TransactionID: transactionID,
						Metadata:      req.Metadata,
					},
				})
			default:
				return nil, errorsInBulk, fmt.Errorf("invalid target type: %s", req.TargetType)
			}
			if err != nil {
				var code string
				switch {
				case errors.Is(err, ledgercontroller.ErrNotFound):
					code = api.ErrorCodeNotFound
				default:
					code = api.ErrorInternal
				}
				bulkError(element.Action, code, err)
				if !continueOnFailure {
					return ret, errorsInBulk, nil
				}
			} else {
				ret = append(ret, Result{
					ResponseType: element.Action,
					LogID:        log.ID,
				})
			}
		case ActionRevertTransaction:
			req := &RevertTransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			log, revertTransactionResult, err := l.RevertTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{
				DryRun:         false,
				IdempotencyKey: element.IdempotencyKey,
				Input: ledgercontroller.RevertTransaction{
					Force:           req.Force,
					AtEffectiveDate: req.AtEffectiveDate,
					TransactionID:   req.ID,
				},
			})
			if err != nil {
				var code string
				switch {
				case errors.Is(err, ledgercontroller.ErrNotFound):
					code = api.ErrorCodeNotFound
				default:
					code = api.ErrorInternal
				}
				bulkError(element.Action, code, err)
				if !continueOnFailure {
					return ret, errorsInBulk, nil
				}
			} else {
				ret = append(ret, Result{
					Data:         revertTransactionResult.RevertTransaction,
					ResponseType: element.Action,
					LogID:        log.ID,
				})
			}
		case ActionDeleteMetadata:
			req := &DeleteMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			var (
				log *ledger.Log
				err error
			)
			switch req.TargetType {
			case ledger.MetaTargetTypeAccount:
				address := ""
				if err := json.Unmarshal(req.TargetID, &address); err != nil {
					return nil, errorsInBulk, err
				}

				log, err = l.DeleteAccountMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.DeleteAccountMetadata]{
					DryRun:         false,
					IdempotencyKey: element.IdempotencyKey,
					Input: ledgercontroller.DeleteAccountMetadata{
						Address: address,
						Key:     req.Key,
					},
				})
			case ledger.MetaTargetTypeTransaction:
				transactionID := 0
				if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
					return nil, errorsInBulk, err
				}

				log, err = l.DeleteTransactionMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]{
					DryRun:         false,
					IdempotencyKey: element.IdempotencyKey,
					Input: ledgercontroller.DeleteTransactionMetadata{
						TransactionID: transactionID,
						Key:           req.Key,
					},
				})
			default:
				return nil, errorsInBulk, fmt.Errorf("unsupported target type: %s", req.TargetType)
			}
			if err != nil {
				var code string
				switch {
				case errors.Is(err, ledgercontroller.ErrNotFound):
					code = api.ErrorCodeNotFound
				default:
					code = api.ErrorInternal
				}
				bulkError(element.Action, code, err)
				if !continueOnFailure {
					return ret, errorsInBulk, nil
				}
			} else {
				ret = append(ret, Result{
					ResponseType: element.Action,
					LogID:        log.ID,
				})
			}
		}
	}
	return ret, errorsInBulk, nil
}
