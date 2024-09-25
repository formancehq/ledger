package v1

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v2/pkg/core/accounts"

	"github.com/go-chi/chi/v5"

	storageerrors "github.com/formancehq/ledger/v2/internal/storage/sqlutils"

	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/ledger/v2/internal/api/backend"
	"github.com/pkg/errors"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/v2/internal"
	"github.com/formancehq/ledger/v2/internal/storage/ledgerstore"
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

	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		sharedapi.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	query := ledgerstore.NewGetAccountQuery(param)
	query = query.WithExpandVolumes()

	acc, err := l.GetAccountWithVolumes(r.Context(), query)
	if err != nil {
		switch {
		case storageerrors.IsNotFoundError(err):
			acc = &ledger.ExpandedAccount{
				Account: ledger.Account{
					Address:  param,
					Metadata: map[string]string{},
				},
				Volumes:          map[string]*ledger.Volumes{},
				EffectiveVolumes: map[string]*ledger.Volumes{},
			}
		default:
			sharedapi.InternalServerError(w, r, err)
			return
		}
	}

	sharedapi.Ok(w, accountWithVolumesAndBalances(*acc))
}

func postAccountMetadata(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())
	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		sharedapi.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	if !accounts.ValidateAddress(param) {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid account address format"))
		return
	}

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		sharedapi.BadRequest(w, ErrValidation, errors.New("invalid metadata format"))
		return
	}

	err = l.SaveMeta(r.Context(), getCommandParameters(r), ledger.MetaTargetTypeAccount, param, m)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}

func deleteAccountMetadata(w http.ResponseWriter, r *http.Request) {
	param, err := url.PathUnescape(chi.URLParam(r, "address"))
	if err != nil {
		sharedapi.BadRequestWithDetails(w, ErrValidation, err, err.Error())
		return
	}

	if err := backend.LedgerFromContext(r.Context()).
		DeleteMetadata(
			r.Context(),
			getCommandParameters(r),
			ledger.MetaTargetTypeAccount,
			param,
			chi.URLParam(r, "key"),
		); err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
