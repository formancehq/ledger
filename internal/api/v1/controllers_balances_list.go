package v1

import (
	"math/big"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/common"
)

func getBalances(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	rq, err := getOffsetPaginatedQuery[any](r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	rq.Options.Builder, err = buildAccountsFilterQuery(r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}
	rq.Options.Expand = []string{"volumes"}

	cursor, err := l.ListAccounts(r.Context(), *rq)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	ret := make([]map[string]map[string]*big.Int, 0)
	for _, item := range cursor.Data {
		e := map[string]map[string]*big.Int{
			item.Address: {},
		}
		for asset, volumes := range item.Volumes {
			e[item.Address][asset] = volumes.Balance()
		}
		ret = append(ret, e)
	}

	api.RenderCursor(w, bunpaginate.Cursor[map[string]map[string]*big.Int]{
		PageSize: cursor.PageSize,
		HasMore:  cursor.HasMore,
		Previous: cursor.Previous,
		Next:     cursor.Next,
		Data:     ret,
	})
}
