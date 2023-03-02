package controllers

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/core"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
)

type MappingController struct{}

func NewMappingController() MappingController {
	return MappingController{}
}

func (ctl *MappingController) PutMapping(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	mapping := &core.Mapping{}
	if err := json.NewDecoder(r.Body).Decode(mapping); err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	if err := l.SaveMapping(r.Context(), *mapping); err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, mapping)
}

func (ctl *MappingController) GetMapping(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	mapping, err := l.LoadMapping(r.Context())
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, mapping)
}
