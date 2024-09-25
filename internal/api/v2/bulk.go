package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/v2/internal/opentelemetry/tracer"

	sharedapi "github.com/formancehq/go-libs/api"

	"github.com/formancehq/ledger/v2/internal/engine"
	"github.com/formancehq/ledger/v2/internal/machine"

	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/v2/internal"
	"github.com/formancehq/ledger/v2/internal/api/backend"
	"github.com/formancehq/ledger/v2/internal/engine/command"
)

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

func ProcessBulk(ctx context.Context, l backend.Ledger, bulk Bulk, continueOnFailure bool) ([]Result, bool, error) {

	ctx, span := tracer.Start(ctx, "Bulk")
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
		parameters := command.Parameters{
			DryRun:         false,
			IdempotencyKey: element.IdempotencyKey,
		}

		switch element.Action {
		case ActionCreateTransaction:
			req := &ledger.TransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}
			rs := req.ToRunScript()

			tx, err := l.CreateTransaction(ctx, parameters, *rs)
			if err != nil {
				var code string
				switch {
				case machine.IsInsufficientFundError(err):
					code = ErrInsufficientFund
				case engine.IsCommandError(err):
					code = ErrValidation
				default:
					code = sharedapi.ErrorInternal
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

			var targetID any
			switch req.TargetType {
			case ledger.MetaTargetTypeAccount:
				targetID = ""
			case ledger.MetaTargetTypeTransaction:
				targetID = big.NewInt(0)
			}
			if err := json.Unmarshal(req.TargetID, &targetID); err != nil {
				return nil, errorsInBulk, err
			}

			if err := l.SaveMeta(ctx, parameters, req.TargetType, targetID, req.Metadata); err != nil {
				var code string
				switch {
				case command.IsSaveMetaError(err, command.ErrSaveMetaCodeTransactionNotFound):
					code = sharedapi.ErrorCodeNotFound
				default:
					code = sharedapi.ErrorInternal
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
				ID              *big.Int `json:"id"`
				Force           bool     `json:"force"`
				AtEffectiveDate bool     `json:"atEffectiveDate"`
			}
			req := &revertTransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			tx, err := l.RevertTransaction(ctx, parameters, req.ID, req.Force, req.AtEffectiveDate)
			if err != nil {
				var code string
				switch {
				case engine.IsCommandError(err):
					code = ErrValidation
				default:
					code = sharedapi.ErrorInternal
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

			var targetID any
			switch req.TargetType {
			case ledger.MetaTargetTypeAccount:
				targetID = ""
			case ledger.MetaTargetTypeTransaction:
				targetID = big.NewInt(0)
			}
			if err := json.Unmarshal(req.TargetID, &targetID); err != nil {
				return nil, errorsInBulk, err
			}

			err := l.DeleteMetadata(ctx, parameters, req.TargetType, targetID, req.Key)
			if err != nil {
				var code string
				switch {
				case command.IsDeleteMetaError(err, command.ErrSaveMetaCodeTransactionNotFound):
					code = sharedapi.ErrorCodeNotFound
				default:
					code = sharedapi.ErrorInternal
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
