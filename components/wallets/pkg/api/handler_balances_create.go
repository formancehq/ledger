package api

import (
	"errors"
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/render"
)

func (m *MainHandler) createBalanceHandler(w http.ResponseWriter, r *http.Request) {
	data := &wallet.CreateBalance{}
	if r.ContentLength > 0 {
		if err := render.Bind(r, data); err != nil {
			badRequest(w, ErrorCodeValidation, err)
			return
		}
	}

	balance, err := m.manager.CreateBalance(r.Context(), data)
	if err != nil {
		switch {
		case errors.Is(err, wallet.ErrInvalidBalanceName):
			fallthrough
		case errors.Is(err, wallet.ErrReservedBalanceName):
			fallthrough
		case errors.Is(err, wallet.ErrBalanceAlreadyExists):
			badRequest(w, ErrorCodeValidation, err)
		default:
			internalError(w, r, err)
		}
		return
	}

	created(w, balance)
}
