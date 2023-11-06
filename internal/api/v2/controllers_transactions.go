package v2

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	"github.com/formancehq/ledger/internal/api/shared"

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
		sharedapi.BadRequest(w, shared.ErrValidation, err)
		return
	}

	count, err := shared.LedgerFromContext(r.Context()).
		CountTransactions(r.Context(), ledgerstore.NewGetTransactionsQuery(*options))
	if err != nil {
		shared.ResponseError(w, r, err)
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
			shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
				errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}
	} else {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			sharedapi.BadRequest(w, shared.ErrValidation, err)
			return
		}
		query = ledgerstore.NewGetTransactionsQuery(*options)
	}

	cursor, err := l.GetTransactions(r.Context(), query)
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

func postTransaction(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	payload := ledger.TransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		shared.ResponseError(w, r,
			errorsutil.NewError(command.ErrValidation,
				errors.New("invalid transaction format")))
		return
	}

	rs, err := payload.ToRunScript()
	if err != nil {
		shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation, err))
		return
	}

	res, err := l.CreateTransaction(r.Context(), getCommandParameters(r), *rs)
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func getTransaction(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	txId, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
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
		sharedapi.BadRequest(w, shared.ErrValidation, err)
		return
	}
	query.PITFilter = *pitFilter

	tx, err := l.GetTransactionWithVolumes(r.Context(), query)
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, tx)
}

func revertTransaction(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	transactionID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.NotFound(w)
		return
	}

	tx, err := l.RevertTransaction(r.Context(), getCommandParameters(r), transactionID, sharedapi.QueryParamBool(r, "force"))
	if err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.Created(w, tx)
}

func postTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid metadata format")))
		return
	}

	txID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.NotFound(w)
		return
	}

	if err := l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeTransaction, txID, m); err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	transactionID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		shared.ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid transaction ID")))
		return
	}

	metadataKey := chi.URLParam(r, "key")

	if err := l.DeleteMetadata(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeTransaction, transactionID, metadataKey); err != nil {
		shared.ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
