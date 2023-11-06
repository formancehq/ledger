package bulk

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/api/shared"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/stack/libs/go-libs/metadata"
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
	ret := make([]Result, 0, len(bulk))

	errorsInBulk := false
	var bulkError = func(action string, err error) {
		_, code, details := shared.CoreErrorToErrorCode(err)
		ret = append(ret, Result{
			ErrorCode:        code,
			ErrorDescription: err.Error(),
			ErrorDetails:     details,
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
			rs, err := req.ToRunScript()
			if err != nil {
				return nil, errorsInBulk, fmt.Errorf("error converting request to script: %s", err)
			}

			tx, err := l.CreateTransaction(ctx, parameters, *rs)
			if err != nil {
				bulkError(element.Action, err)
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
				bulkError(element.Action, err)
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
				ID *big.Int `json:"id"`
			}
			req := &revertTransactionRequest{}
			if err := json.Unmarshal(element.Data, req); err != nil {
				return nil, errorsInBulk, fmt.Errorf("error parsing element %d: %s", i, err)
			}

			tx, err := l.RevertTransaction(ctx, parameters, req.ID)
			if err != nil {
				bulkError(element.Action, err)
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
				bulkError(element.Action, err)
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
