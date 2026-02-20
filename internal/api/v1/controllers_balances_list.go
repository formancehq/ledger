package v1

import (
	"math/big"
	"net/http"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func getBalances(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	rq, err := getPaginatedQuery[any](
		r,
		"address",
		bunpaginate.OrderAsc,
		func(resourceQuery *storagecommon.ResourceQuery[any]) error {
			var err error
			resourceQuery.Expand = append(resourceQuery.Expand, "volumes")
			resourceQuery.Builder, err = buildAccountsFilterQuery(r)
			return err
		},
	)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	cursor, err := l.ListAccounts(r.Context(), rq)
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
