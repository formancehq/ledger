package v2

import (
	"encoding/json"
	"io"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"
	"github.com/formancehq/go-libs/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
)

func importLogs(w http.ResponseWriter, r *http.Request) {

	stream := make(chan ledger.Log)
	errChan := make(chan error, 1)
	go func() {
		errChan <- common.LedgerFromContext(r.Context()).Import(r.Context(), stream)
	}()
	dec := json.NewDecoder(r.Body)
	handleError := func(err error) {
		switch {
		case errors.Is(err, ledgercontroller.ErrImport{}):
			api.BadRequest(w, "IMPORT", err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
	}
	for {
		l := ledger.Log{}
		if err := dec.Decode(&l); err != nil {
			if errors.Is(err, io.EOF) {
				close(stream)
				break
			} else {
				api.InternalServerError(w, r, err)
				return
			}
		}
		select {
		case stream <- l:
		case <-r.Context().Done():
			api.InternalServerError(w, r, r.Context().Err())
			return
		case err := <-errChan:
			handleError(err)
			return
		}
	}
	select {
	case err := <-errChan:
		if err != nil {
			handleError(err)
			return
		}
	case <-r.Context().Done():
		api.InternalServerError(w, r, r.Context().Err())
		return
	}

	api.NoContent(w)
}
