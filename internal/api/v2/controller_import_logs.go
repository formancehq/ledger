package v2

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/formancehq/ledger/v2/internal/engine"

	"github.com/formancehq/go-libs/api"
	ledger "github.com/formancehq/ledger/v2/internal"
	"github.com/formancehq/ledger/v2/internal/api/backend"
	"github.com/pkg/errors"
)

func importLogs(w http.ResponseWriter, r *http.Request) {

	stream := make(chan *ledger.ChainedLog)
	errChan := make(chan error, 1)
	go func() {
		errChan <- backend.LedgerFromContext(r.Context()).Import(r.Context(), stream)
	}()
	dec := json.NewDecoder(r.Body)
	handleError := func(err error) {
		switch {
		case errors.Is(err, engine.ImportError{}):
			api.WriteErrorResponse(w, http.StatusBadRequest, "IMPORT", err)
		default:
			api.InternalServerError(w, r, err)
		}
	}
	for {
		l := &ledger.ChainedLog{}
		if err := dec.Decode(l); err != nil {
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
