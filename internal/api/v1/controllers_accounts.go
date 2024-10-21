package v1

import (
	"encoding/json"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"errors"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
)

type accountWithVolumesAndBalances ledger.Account

func (a accountWithVolumesAndBalances) MarshalJSON() ([]byte, error) {
	type aux struct {
		ledger.Account
		Balances map[string]*big.Int `json:"balances"`
	}
	return json.Marshal(aux{
		Account:  ledger.Account(a),
		Balances: a.Volumes.Balances(),
	})
}

func buildAccountsFilterQuery(r *http.Request) (query.Builder, error) {
	clauses := make([]query.Builder, 0)

	if balance := r.URL.Query().Get("balance"); balance != "" {
		balanceValue, err := strconv.ParseInt(balance, 10, 64)
		if err != nil {
			return nil, err
		}

		switch getBalanceOperator(r) {
		case "e":
			clauses = append(clauses, query.Match("balance", balanceValue))
		case "ne":
			clauses = append(clauses, query.Not(query.Match("balance", balanceValue)))
		case "lt":
			clauses = append(clauses, query.Lt("balance", balanceValue))
		case "lte":
			clauses = append(clauses, query.Lte("balance", balanceValue))
		case "gt":
			clauses = append(clauses, query.Gt("balance", balanceValue))
		case "gte":
			clauses = append(clauses, query.Gte("balance", balanceValue))
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
	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return query.And(clauses...), nil
}
