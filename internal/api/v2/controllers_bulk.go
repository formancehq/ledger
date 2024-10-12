package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func bulkHandler(w http.ResponseWriter, r *http.Request) {
	b := Bulk{}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		api.BadRequest(w, ErrValidation, err)
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

const (
	ActionCreateTransaction = "CREATE_TRANSACTION"
	ActionAddMetadata       = "ADD_METADATA"
	ActionRevertTransaction = "REVERT_TRANSACTION"
	ActionDeleteMetadata    = "DELETE_METADATA"
)

type Bulk []Element

type Element struct {
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

			createTransactionResult, err := l.CreateTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.RunScript]{
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
				})
			}
		case ActionAddMetadata:
			type addMetadataRequest struct {
				TargetType string            `json:"targetType"`
				TargetID   json.RawMessage   `json:"targetId"`
				Metadata   metadata.Metadata `json:"metadata"`
			}
			req := &addMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			var err error
			switch req.TargetType {
			case ledger.MetaTargetTypeAccount:
				address := ""
				if err := json.Unmarshal(req.TargetID, &address); err != nil {
					return nil, errorsInBulk, err
				}
				err = l.SaveAccountMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
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
				err = l.SaveTransactionMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]{
					DryRun:         false,
					IdempotencyKey: element.IdempotencyKey,
					Input: ledgercontroller.SaveTransactionMetadata{
						TransactionID: transactionID,
						Metadata:      req.Metadata,
					},
				})
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
				})
			}
		case ActionRevertTransaction:
			type revertTransactionRequest struct {
				ID              int  `json:"id"`
				Force           bool `json:"force"`
				AtEffectiveDate bool `json:"atEffectiveDate"`
			}
			req := &revertTransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			revertTransactionResult, err := l.RevertTransaction(ctx, ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{
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
				})
			}
		case ActionDeleteMetadata:
			type deleteMetadataRequest struct {
				TargetType string          `json:"targetType"`
				TargetID   json.RawMessage `json:"targetId"`
				Key        string          `json:"key"`
			}
			req := &deleteMetadataRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			var err error
			switch req.TargetType {
			case ledger.MetaTargetTypeAccount:
				address := ""
				if err := json.Unmarshal(req.TargetID, &address); err != nil {
					return nil, errorsInBulk, err
				}
				err = l.DeleteAccountMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.DeleteAccountMetadata]{
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
				err = l.DeleteTransactionMetadata(ctx, ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]{
					DryRun:         false,
					IdempotencyKey: element.IdempotencyKey,
					Input: ledgercontroller.DeleteTransactionMetadata{
						TransactionID: transactionID,
						Key:           req.Key,
					},
				})
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
				})
			}
		}
	}
	return ret, errorsInBulk, nil
}
