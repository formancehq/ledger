package api

import (
	"errors"
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/chi/v5"
)

func (m *MainHandler) getBalanceHandler(w http.ResponseWriter, r *http.Request) {
	balance, err := m.manager.GetBalance(r.Context(), chi.URLParam(r, "walletID"), chi.URLParam(r, "balanceName"))
	if err != nil {
		switch {
		case errors.Is(err, wallet.ErrBalanceNotExists):
			notFound(w)
		default:
			internalError(w, r, err)
		}
		return
	}

	ok(w, balance)
}
