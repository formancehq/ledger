package api

import (
	"errors"
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func (m *MainHandler) patchWalletHandler(w http.ResponseWriter, r *http.Request) {
	data := &wallet.PatchRequest{}
	if err := render.Bind(r, data); err != nil {
		badRequest(w, ErrorCodeValidation, err)
		return
	}

	err := m.manager.UpdateWallet(r.Context(), chi.URLParam(r, "walletID"), data)
	if err != nil {
		switch {
		case errors.Is(err, wallet.ErrWalletNotFound):
			notFound(w)
		default:
			internalError(w, r, err)
		}
		return
	}

	noContent(w)
}
