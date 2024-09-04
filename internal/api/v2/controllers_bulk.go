package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"
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

func ProcessBulk(ctx context.Context, l ledgercontroller.Controller, bulk Bulk, continueOnFailure bool) ([]Result, bool, error) {

	ctx, span := tracing.Start(ctx, "Bulk")
	defer span.End()

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
		parameters := ledgercontroller.Parameters{
			DryRun:         false,
			IdempotencyKey: element.IdempotencyKey,
		}

		switch element.Action {
		case ActionCreateTransaction:
			req := &ledger.TransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}
			rs := req.ToRunScript(false)

			tx, err := l.CreateTransaction(ctx, parameters, *rs)
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
				case errors.Is(err, ledgercontroller.ErrReferenceConflict{}):
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
					Data:         tx,
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
				targetID := ""
				if err := json.Unmarshal(req.TargetID, &targetID); err != nil {
					return nil, errorsInBulk, err
				}
				err = l.SaveAccountMetadata(ctx, parameters, targetID, req.Metadata)
			case ledger.MetaTargetTypeTransaction:
				targetID := 0
				if err := json.Unmarshal(req.TargetID, &targetID); err != nil {
					return nil, errorsInBulk, err
				}
				err = l.SaveTransactionMetadata(ctx, parameters, targetID, req.Metadata)
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

			tx, err := l.RevertTransaction(ctx, parameters, req.ID, req.Force, req.AtEffectiveDate)
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
					Data:         tx,
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
				targetID := ""
				if err := json.Unmarshal(req.TargetID, &targetID); err != nil {
					return nil, errorsInBulk, err
				}
				err = l.DeleteAccountMetadata(ctx, parameters, targetID, req.Key)
			case ledger.MetaTargetTypeTransaction:
				targetID := 0
				if err := json.Unmarshal(req.TargetID, &targetID); err != nil {
					return nil, errorsInBulk, err
				}
				err = l.DeleteTransactionMetadata(ctx, parameters, targetID, req.Key)
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
