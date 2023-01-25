package api

import (
	"errors"
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/chi/v5"
)

func (m *MainHandler) getWalletHandler(wr http.ResponseWriter, r *http.Request) {
	w, err := m.manager.GetWallet(r.Context(), chi.URLParam(r, "walletID"))
	if err != nil {
		switch {
		case errors.Is(err, wallet.ErrWalletNotFound):
			notFound(wr)
		default:
			internalError(wr, r, err)
		}
		return
	}

	ok(wr, w)
}
