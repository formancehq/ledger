package v1

import (
	"encoding/json"
	"fmt"
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

func buildAccountsFilterQuery(r *http.Request) (query.Builder, error) {
	clauses := make([]query.Builder, 0)

	if balance := r.URL.Query().Get("balance"); balance != "" {
		if _, err := strconv.ParseInt(balance, 10, 64); err != nil {
			return nil, err
		}

		balanceOperator, err := getBalanceOperator(r)
		if err != nil {
			return nil, err
		}

		switch balanceOperator {
		case "e":
			clauses = append(clauses, query.Match("balance", balance))
		case "ne":
			clauses = append(clauses, query.Not(query.Match("balance", balance)))
		case "lt":
			clauses = append(clauses, query.Lt("balance", balance))
		case "lte":
			clauses = append(clauses, query.Lte("balance", balance))
		case "gt":
			clauses = append(clauses, query.Gt("balance", balance))
		case "gte":
			clauses = append(clauses, query.Gte("balance", balance))
		default:
			return nil, errors.New("invalid balance operator")
		}
	}

	if address := r.URL.Query().Get("address"); address != "" {
		clauses = append(clauses, query.Match("address", address))
	}

	for elem, value := range r.URL.Query() {
		if strings.HasPrefix(elem, "metadata") {
			clauses = append(clauses, query.Match(elem, value[0]))
		}
	}

	if len(clauses) == 0 {
		return nil, nil
	}

	return query.And(clauses...), nil
}

func countAccounts(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := l.CountAccounts(r.Context(), ledgerstore.NewGetAccountsQuery(*options))
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func getAccounts(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	q := &ledgerstore.GetAccountsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := paginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), q)
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
		options.QueryBuilder, err = buildAccountsFilterQuery(r)
		q = ledgerstore.NewGetAccountsQuery(*options)
	}

	cursor, err := l.GetAccountsWithVolumes(r.Context(), q)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

func getAccount(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	query := ledgerstore.NewGetAccountQuery(chi.URLParam(r, "address"))
	if collectionutils.Contains(r.URL.Query()["expand"], "volumes") {
		query = query.WithExpandVolumes()
	}
	if collectionutils.Contains(r.URL.Query()["expand"], "effectiveVolumes") {
		query = query.WithExpandEffectiveVolumes()
	}

	acc, err := l.GetAccountWithVolumes(r.Context(), query)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, acc)
}

func postAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := shared.LedgerFromContext(r.Context())

	if !ledger.ValidateAddress(chi.URLParam(r, "address")) {
		ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid account address format")))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
			errors.New("invalid metadata format")))
		return
	}

	err := l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeAccount, chi.URLParam(r, "address"), m)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	if err := shared.LedgerFromContext(r.Context()).
		DeleteMetadata(
			r.Context(),
			getCommandParameters(r),
			ledger.MetaTargetTypeAccount,
			chi.URLParam(r, "address"),
			chi.URLParam(r, "key"),
		); err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
