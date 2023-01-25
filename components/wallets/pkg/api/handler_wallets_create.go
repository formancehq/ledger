package api

import (
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/render"
)

func (m *MainHandler) createWalletHandler(w http.ResponseWriter, r *http.Request) {
	data := &wallet.CreateRequest{}
	if r.ContentLength > 0 {
		if err := render.Bind(r, data); err != nil {
			badRequest(w, ErrorCodeValidation, err)
			return
		}
	}

	wallet, err := m.manager.CreateWallet(r.Context(), data)
	if err != nil {
		internalError(w, r, err)
		return
	}

	ok(w, wallet)
}
