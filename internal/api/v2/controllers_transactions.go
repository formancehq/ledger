package v2

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func countTransactions(w http.ResponseWriter, r *http.Request) {

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := LedgerFromContext(r.Context()).
		CountTransactions(r.Context(), ledgerstore.NewGetTransactionsQuery(*options))
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func getTransactions(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	query := &ledgerstore.GetTransactionsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := paginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &query)
		if err != nil {
			ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
				errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}
	} else {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, err)
			return
		}
		query = ledgerstore.NewGetTransactionsQuery(*options)
	}

	cursor, err := l.GetTransactions(r.Context(), query)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

type Script struct {
	ledger.Script
	Vars map[string]any `json:"vars"`
}

func (s Script) ToCore() ledger.Script {
	s.Script.Vars = map[string]string{}
	for k, v := range s.Vars {
		switch v := v.(type) {
		case string:
			s.Script.Vars[k] = v
		case map[string]any:
			s.Script.Vars[k] = fmt.Sprintf("%s %v", v["asset"], v["amount"])
		default:
			s.Script.Vars[k] = fmt.Sprint(v)
		}
	}
	return s.Script
}

type PostTransactionRequest struct {
	Postings  ledger.Postings   `json:"postings"`
	Script    Script            `json:"script"`
	Timestamp ledger.Time       `json:"timestamp"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata" swaggertype:"object"`
}

func postTransaction(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	payload := PostTransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		ResponseError(w, r,
			errorsutil.NewError(command.ErrValidation,
				errors.New("invalid transaction format")))
		return
	}

	if len(payload.Postings) > 0 && payload.Script.Plain != "" ||
		len(payload.Postings) == 0 && payload.Script.Plain == "" {
		ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid payload: should contain either postings or script")))
		return
	} else if len(payload.Postings) > 0 {
		if i, err := payload.Postings.Validate(); err != nil {
			ResponseError(w, r, errorsutil.NewError(command.ErrValidation, errors.Wrap(err,
				fmt.Sprintf("invalid posting %d", i))))
			return
		}
		txData := ledger.TransactionData{
			Postings:  payload.Postings,
			Timestamp: payload.Timestamp,
			Reference: payload.Reference,
			Metadata:  payload.Metadata,
		}

		res, err := l.CreateTransaction(r.Context(), getCommandParameters(r), ledger.TxToScriptData(txData))
		if err != nil {
			ResponseError(w, r, err)
			return
		}

		sharedapi.Ok(w, res)
		return
	}

	script := ledger.RunScript{
		Script:    payload.Script.ToCore(),
		Timestamp: payload.Timestamp,
		Reference: payload.Reference,
		Metadata:  payload.Metadata,
	}

	res, err := l.CreateTransaction(r.Context(), getCommandParameters(r), script)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func getTransaction(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	txId, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid transaction ID")))
		return
	}

	query := ledgerstore.NewGetTransactionQuery(txId)
	if collectionutils.Contains(r.URL.Query()["expand"], "volumes") {
		query = query.WithExpandVolumes()
	}
	if collectionutils.Contains(r.URL.Query()["expand"], "effectiveVolumes") {
		query = query.WithExpandEffectiveVolumes()
	}

	pitFilter, err := getPITFilter(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}
	query.PITFilter = *pitFilter

	tx, err := l.GetTransactionWithVolumes(r.Context(), query)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, tx)
}

func revertTransaction(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	transactionID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.NotFound(w)
		return
	}

	tx, err := l.RevertTransaction(r.Context(), getCommandParameters(r), transactionID)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Created(w, tx)
}

func postTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid metadata format")))
		return
	}

	txID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.NotFound(w)
		return
	}

	if err := l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeTransaction, txID, m); err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	transactionID, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid transaction ID")))
		return
	}

	metadataKey := chi.URLParam(r, "key")

	if err := l.DeleteMetadata(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeTransaction, transactionID, metadataKey); err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
