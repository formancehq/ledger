package api

import (
	"errors"
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

const (
	ErrorCodeInternal         = "INTERNAL"
	ErrorCodeInsufficientFund = "INSUFFICIENT_FUND"
	ErrorCodeValidation       = "VALIDATION"
	ErrorCodeClosedHold       = "HOLD_CLOSED"
)

func (m *MainHandler) creditWalletHandler(w http.ResponseWriter, r *http.Request) {
	data := &wallet.CreditRequest{}
	if err := render.Bind(r, data); err != nil {
		badRequest(w, ErrorCodeValidation, err)
		return
	}

	id := chi.URLParam(r, "walletID")
	credit := wallet.Credit{
		WalletID:      id,
		CreditRequest: *data,
	}

	err := m.manager.Credit(r.Context(), credit)
	if err != nil {
		switch {
		case errors.Is(err, wallet.ErrBalanceNotExists):
			badRequest(w, ErrorCodeValidation, err)
		default:
			internalError(w, r, err)
		}
		return
	}

	noContent(w)
}
