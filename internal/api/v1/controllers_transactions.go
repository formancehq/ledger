package v1

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/internal/api/shared"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func mapTransactionToV1(tx ledger.Transaction) any {
	return struct {
		ledger.Transaction
		TxID *big.Int `json:"txid"`
		ID   *big.Int `json:"-"`
	}{
		Transaction: tx,
		TxID:        tx.ID,
	}
}

func mapExpandedTransactionToV1(tx ledger.ExpandedTransaction) any {
	return struct {
		ledger.ExpandedTransaction
		TxID *big.Int `json:"txid"`
		ID   *big.Int `json:"-"`
	}{
		ExpandedTransaction: tx,
		TxID:                tx.ID,
	}
}

func buildGetTransactionsQuery(r *http.Request) (query.Builder, error) {
	clauses := make([]query.Builder, 0)
	if after := r.URL.Query().Get("after"); after != "" {
		clauses = append(clauses, query.Lt("id", after))
	}

	if startTime := r.URL.Query().Get("start_time"); startTime != "" {
		clauses = append(clauses, query.Gte("date", startTime))
	}
	if endTime := r.URL.Query().Get("end_time"); endTime != "" {
		clauses = append(clauses, query.Lt("date", endTime))
	}

	if reference := r.URL.Query().Get("reference"); reference != "" {
		clauses = append(clauses, query.Match("reference", reference))
	}
	if source := r.URL.Query().Get("source"); source != "" {
		clauses = append(clauses, query.Match("source", source))
	}
	if destination := r.URL.Query().Get("destination"); destination != "" {
		clauses = append(clauses, query.Match("destination", destination))
	}
	if address := r.URL.Query().Get("account"); address != "" {
		clauses = append(clauses, query.Match("account", address))
	}
	for elem, value := range r.URL.Query() {
		if strings.HasPrefix(elem, "metadata") {
			clauses = append(clauses, query.Match(elem, value[0]))
		}
	}

	if len(clauses) == 0 {
		return nil, nil
	}
	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return query.And(clauses...), nil
}

func countTransactions(w http.ResponseWriter, r *http.Request) {

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}
	options.QueryBuilder, err = buildGetTransactionsQuery(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := shared.LedgerFromContext(r.Context()).
		CountTransactions(r.Context(), ledgerstore.NewGetTransactionsQuery(*options))
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func getTransactions(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

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
		options.QueryBuilder, err = buildGetTransactionsQuery(r)
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

	sharedapi.RenderCursor(w, *sharedapi.MapCursor(cursor, mapExpandedTransactionToV1))
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
	l := shared.LedgerFromContext(r.Context())

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

		sharedapi.Ok(w, []any{mapTransactionToV1(*res)})
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

	sharedapi.Ok(w, []any{mapTransactionToV1(*res)})
}

func getTransaction(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

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

	tx, err := l.GetTransactionWithVolumes(r.Context(), query)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, mapExpandedTransactionToV1(*tx))
}

func revertTransaction(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

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

	sharedapi.Created(w, mapTransactionToV1(*tx))
}

func postTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

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
	l := shared.LedgerFromContext(r.Context())

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
