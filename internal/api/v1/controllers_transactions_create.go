package v1

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
)

type Script struct {
	ledgercontroller.Script
	Vars map[string]json.RawMessage `json:"vars"`
}

func (s Script) ToCore() (*ledgercontroller.Script, error) {
	s.Script.Vars = map[string]string{}
	for k, v := range s.Vars {

		m := make(map[string]json.RawMessage)
		if err := json.Unmarshal(v, &m); err != nil {
			var rawValue string
			if err := json.Unmarshal(v, &rawValue); err != nil {
				panic(err)
			}
			s.Script.Vars[k] = rawValue
			continue
		}

		// Is a monetary
		var asset string
		if err := json.Unmarshal(m["asset"], &asset); err != nil {
			return nil, fmt.Errorf("unmarshalling asset: %w", err)
		}
		amount := &big.Int{}
		if err := json.Unmarshal(m["amount"], amount); err != nil {
			return nil, fmt.Errorf("unmarshalling amount: %w", err)
		}

		s.Script.Vars[k] = fmt.Sprintf("%s %s", asset, amount)
	}
	return &s.Script, nil
}

type CreateTransactionRequest struct {
	Postings  ledger.Postings   `json:"postings"`
	Script    Script            `json:"script"`
	Timestamp time.Time         `json:"timestamp"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata" swaggertype:"object"`
}

func createTransaction(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	payload := CreateTransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		api.BadRequest(w, common.ErrValidation, errors.New("invalid transaction format"))
		return
	}

	if len(payload.Postings) > 0 && payload.Script.Plain != "" ||
		len(payload.Postings) == 0 && payload.Script.Plain == "" {
		api.BadRequest(w, common.ErrValidation, errors.New("invalid payload: should contain either postings or script"))
		return
	} else if len(payload.Postings) > 0 {
		if _, err := payload.Postings.Validate(); err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}
		txData := ledger.TransactionData{
			Postings:  payload.Postings,
			Timestamp: payload.Timestamp,
			Reference: payload.Reference,
			Metadata:  payload.Metadata,
		}

		_, res, err := l.CreateTransaction(r.Context(), getCommandParameters(r, ledgercontroller.TxToScriptData(txData, false)))
		if err != nil {
			switch {
			case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}):
				api.BadRequest(w, common.ErrInsufficientFund, err)
			case errors.Is(err, &ledgercontroller.ErrInvalidVars{}) || errors.Is(err, ledgercontroller.ErrCompilationFailed{}):
				api.BadRequest(w, common.ErrScriptCompilationFailed, err)
			case errors.Is(err, &ledgercontroller.ErrMetadataOverride{}):
				api.BadRequest(w, common.ErrScriptMetadataOverride, err)
			case errors.Is(err, ledgercontroller.ErrNoPostings):
				api.BadRequest(w, common.ErrValidation, err)
			case errors.Is(err, ledgercontroller.ErrTransactionReferenceConflict{}):
				api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}
		api.Ok(w, []any{mapTransactionToV1(res.Transaction)})
		return
	}

	script, err := payload.Script.ToCore()
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	runScript := ledgercontroller.RunScript{
		Script:    *script,
		Timestamp: payload.Timestamp,
		Reference: payload.Reference,
		Metadata:  payload.Metadata,
	}

	_, res, err := l.CreateTransaction(r.Context(), getCommandParameters(r, runScript))
	if err != nil {
		switch {
		case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}):
			api.BadRequest(w, common.ErrInsufficientFund, err)
		case errors.Is(err, &ledgercontroller.ErrInvalidVars{}) ||
			errors.Is(err, ledgercontroller.ErrCompilationFailed{}) ||
			errors.Is(err, &ledgercontroller.ErrMetadataOverride{}) ||
			errors.Is(err, ledgercontroller.ErrNoPostings):
			api.BadRequest(w, common.ErrValidation, err)
		case errors.Is(err, ledgercontroller.ErrTransactionReferenceConflict{}):
			api.WriteErrorResponse(w, http.StatusConflict, common.ErrConflict, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.Ok(w, []any{mapTransactionToV1(res.Transaction)})
}
