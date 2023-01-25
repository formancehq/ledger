package api

import (
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
)

func (m *MainHandler) listWalletsHandler(w http.ResponseWriter, r *http.Request) {
	query := readPaginatedRequest[wallet.ListWallets](r, func(r *http.Request) wallet.ListWallets {
		return wallet.ListWallets{
			Metadata: getQueryMap(r.URL.Query(), "metadata"),
			Name:     r.URL.Query().Get("name"),
		}
	})
	response, err := m.manager.ListWallets(r.Context(), query)
	if err != nil {
		internalError(w, r, err)
		return
	}

	cursorFromListResponse(w, query, response)
}
