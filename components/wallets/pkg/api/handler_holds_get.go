package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (m *MainHandler) getHoldHandler(w http.ResponseWriter, r *http.Request) {
	hold, err := m.manager.GetHold(r.Context(), chi.URLParam(r, "holdID"))
	if err != nil {
		internalError(w, r, err)
		return
	}

	ok(w, hold)
}
