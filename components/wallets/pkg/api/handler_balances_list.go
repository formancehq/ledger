package api

import (
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/chi/v5"
)

func (m *MainHandler) listBalancesHandler(w http.ResponseWriter, r *http.Request) {
	query := readPaginatedRequest(r, func(r *http.Request) wallet.ListBalances {
		return wallet.ListBalances{
			WalletID: chi.URLParam(r, "walletID"),
			Metadata: getQueryMap(r.URL.Query(), "metadata"),
		}
	})

	holds, err := m.manager.ListBalances(r.Context(), query)
	if err != nil {
		internalError(w, r, err)
		return
	}

	cursorFromListResponse(w, query, holds)
}
