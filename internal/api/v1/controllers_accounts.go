package v1

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/formancehq/stack/libs/core/accounts"
	"github.com/formancehq/stack/libs/go-libs/pointer"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"
	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/pkg/errors"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
)

type accountWithVolumesAndBalances ledger.ExpandedAccount

func (a accountWithVolumesAndBalances) MarshalJSON() ([]byte, error) {
	type aux struct {
		ledger.ExpandedAccount
		Balances map[string]*big.Int `json:"balances"`
	}
	return json.Marshal(aux{
		ExpandedAccount: ledger.ExpandedAccount(a),
		Balances:        a.Volumes.Balances(),
	})
}

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
	l := backend.LedgerFromContext(r.Context())

	options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	count, err := l.CountAccounts(r.Context(), ledgerstore.NewGetAccountsQuery(*options))
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	sharedapi.NoContent(w)
}

func getAccounts(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	query, err := bunpaginate.Extract[ledgerstore.GetAccountsQuery](r, func() (*ledgerstore.GetAccountsQuery, error) {
		options, err := getPaginatedQueryOptionsOfPITFilterWithVolumes(r)
		if err != nil {
			return nil, err
		}
		options.QueryBuilder, err = buildAccountsFilterQuery(r)
		return pointer.For(ledgerstore.NewGetAccountsQuery(*options)), nil
	})
	if err != nil {
		sharedapi.BadRequest(w, ErrValidation, err)
		return
	}

	cursor, err := l.GetAccountsWithVolumes(r.Context(), *query)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}

func getAccount(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	query := ledgerstore.NewGetAccountQuery(chi.URLParam(r, "address"))
	query = query.WithExpandVolumes()

	acc, err := l.GetAccountWithVolumes(r.Context(), query)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, accountWithVolumesAndBalances(*acc))
}

func postAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	if !accounts.ValidateAddress(chi.URLParam(r, "address")) {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid account address format"))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid metadata format"))
		return
	}

	err := l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeAccount, chi.URLParam(r, "address"), m)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	if err := backend.LedgerFromContext(r.Context()).
		DeleteMetadata(
			r.Context(),
			getCommandParameters(r),
			ledger.MetaTargetTypeAccount,
			chi.URLParam(r, "address"),
			chi.URLParam(r, "key"),
		); err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
