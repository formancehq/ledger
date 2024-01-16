package v2

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/pointer"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"
	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/engine"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/machine"
	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"
	"github.com/pkg/errors"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

func countTransactions(w http.ResponseWriter, r *http.Request) {

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := backend.LedgerFromContext(r.Context()).
		CountTransactions(r.Context(), ledgerstore.NewGetTransactionsQuery(*options))
	if err != nil {
		switch {
		case ledgerstore.IsErrInvalidQuery(err):
			sharedapi.BadRequest(w, ErrValidation, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func getTransactions(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	query, err := bunpaginate.Extract[ledgerstore.GetTransactionsQuery](r, func() (*ledgerstore.GetTransactionsQuery, error) {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			return nil, err
		}
		return pointer.For(ledgerstore.NewGetTransactionsQuery(*options)), nil
	})
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.GetTransactions(r.Context(), *query)
	if err != nil {
		switch {
		case ledgerstore.IsErrInvalidQuery(err):
			sharedapi.BadRequest(w, ErrValidation, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

func postTransaction(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	payload := ledger.TransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid transaction format"))
		return
	}

	if len(payload.Postings) > 0 && payload.Script.Plain != "" {
		sharedapi.BadRequest(w, ErrValidation, errors.New("cannot pass postings and numscript in the same request"))
		return
	}

	res, err := l.CreateTransaction(r.Context(), getCommandParameters(r), *payload.ToRunScript())
	if err != nil {
		switch {
		case engine.IsCommandError(err):
			switch {
			case command.IsErrMachine(err):
				switch {
				case machine.IsInsufficientFundError(err):
					sharedapi.BadRequest(w, ErrInsufficientFund, err)
					return
				case machine.IsMetadataOverride(err):
					sharedapi.BadRequest(w, ErrMetadataOverride, err)
					return
				}
			case command.IsInvalidTransactionError(err, command.ErrInvalidTransactionCodeConflict):
				sharedapi.BadRequest(w, ErrConflict, err)
				return
			case command.IsInvalidTransactionError(err, command.ErrInvalidTransactionCodeNoPostings):
				sharedapi.BadRequest(w, ErrNoPostings, err)
				return
			case command.IsInvalidTransactionError(err, command.ErrInvalidTransactionCodeCompilationFailed):
				sharedapi.BadRequestWithDetails(w, ErrCompilationFailed, err, backend.EncodeLink(errors.Cause(err).Error()))
				return
			}
		}
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func getTransaction(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	txId, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid transaction id"))
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
		switch {
		case storageerrors.IsNotFoundError(err):
			sharedapi.NotFound(w, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	sharedapi.Ok(w, tx)
}

func revertTransaction(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	transactionID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.NotFound(w, errors.New("invalid transaction ID"))
		return
	}

	tx, err := l.RevertTransaction(r.Context(), getCommandParameters(r), transactionID, sharedapi.QueryParamBool(r, "force"))
	if err != nil {
		switch {
		case engine.IsCommandError(err):
			switch {
			case command.IsErrMachine(err):
				switch {
				case machine.IsInsufficientFundError(err):
					sharedapi.BadRequest(w, ErrInsufficientFund, err)
					return
				}
			case command.IsRevertError(err, command.ErrRevertTransactionCodeNotFound):
				sharedapi.NotFound(w, err)
				return
			case command.IsRevertError(err, command.ErrRevertTransactionCodeOccurring):
				sharedapi.BadRequest(w, ErrRevertOccurring, err)
				return
			case command.IsRevertError(err, command.ErrRevertTransactionCodeAlreadyReverted):
				sharedapi.BadRequest(w, ErrAlreadyRevert, err)
				return
			}
		}
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Created(w, tx)
}

func postTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid metadata format"))
		return
	}

	txID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.NotFound(w, errors.New("invalid transaction ID"))
		return
	}

	if err := l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeTransaction, txID, m); err != nil {
		switch {
		case command.IsSaveMetaError(err, command.ErrSaveMetaCodeTransactionNotFound):
			sharedapi.NotFound(w, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	sharedapi.NoContent(w)
}

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	transactionID, ok := big.NewInt(0).SetString(chi.URLParam(r, "id"), 10)
	if !ok {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid transaction ID"))
		return
	}

	metadataKey := chi.URLParam(r, "key")

	if err := l.DeleteMetadata(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeTransaction, transactionID, metadataKey); err != nil {
		switch {
		case command.IsSaveMetaError(err, command.ErrSaveMetaCodeTransactionNotFound):
			sharedapi.NotFound(w, err)
		default:
			sharedapi.InternalServerError(w, r, err)
		}
		return
	}

	sharedapi.NoContent(w)
}
