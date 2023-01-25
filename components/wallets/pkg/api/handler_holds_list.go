package api

import (
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
)

func (m *MainHandler) listHoldsHandler(w http.ResponseWriter, r *http.Request) {
	query := readPaginatedRequest(r, func(r *http.Request) wallet.ListHolds {
		return wallet.ListHolds{
			WalletID: r.URL.Query().Get("walletID"),
			Metadata: getQueryMap(r.URL.Query(), "metadata"),
		}
	})

	holds, err := m.manager.ListHolds(r.Context(), query)
	if err != nil {
		internalError(w, r, err)
		return
	}

	cursorFromListResponse(w, query, holds)
}
