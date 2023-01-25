package api

import (
	"errors"
	"net/http"

	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

type ConfirmHoldRequest struct {
	Amount int64 `json:"amount"`
	Final  bool  `json:"final"`
}

func (c ConfirmHoldRequest) Bind(r *http.Request) error {
	return nil
}

func (m *MainHandler) confirmHoldHandler(w http.ResponseWriter, r *http.Request) {
	data := &ConfirmHoldRequest{}
	if r.ContentLength > 0 {
		if err := render.Bind(r, data); err != nil {
			badRequest(w, ErrorCodeValidation, err)
			return
		}
	}

	err := m.manager.ConfirmHold(r.Context(), wallet.ConfirmHold{
		HoldID: chi.URLParam(r, "holdID"),
		Amount: *wallet.NewMonetaryInt(data.Amount),
		Final:  data.Final,
	})
	if err != nil {
		switch {
		case errors.Is(err, wallet.ErrHoldNotFound):
			notFound(w)
		case errors.Is(err, wallet.ErrInsufficientFundError):
			badRequest(w, ErrorCodeInsufficientFund, err)
		case errors.Is(err, wallet.ErrClosedHold):
			badRequest(w, ErrorCodeClosedHold, err)
		default:
			internalError(w, r, err)
		}
		return
	}

	noContent(w)
}
