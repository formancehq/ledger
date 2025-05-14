package v2

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
)

func importLogs(w http.ResponseWriter, r *http.Request) {

	stream := make(chan ledger.Log)
	errChan := make(chan error, 1)
	go func() {
		err := common.LedgerFromContext(r.Context()).Import(r.Context(), stream)
		if err != nil {
			err = fmt.Errorf("importing logs: %w", err)
		}
		errChan <- err
	}()
	dec := json.NewDecoder(r.Body)
	handleError := func(err error) {
		switch {
		case errors.Is(err, ledgercontroller.ErrImport{}):
			api.BadRequest(w, "IMPORT", err)
		case errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
	}
	for {
		l := ledger.Log{}
		if err := dec.Decode(&l); err != nil {
			if errors.Is(err, io.EOF) {
				close(stream)
				stream = nil
			} else {
				common.InternalServerError(w, r, fmt.Errorf("reading input stream: %w", err))
				return
			}
		}

		select {
		case stream <- l:
		case <-r.Context().Done():
			common.InternalServerError(w, r, fmt.Errorf("request context done: %w", r.Context().Err()))
			return
		case err := <-errChan:
			if err != nil {
				handleError(err)
				return
			}
			if stream != nil {
				panic("got nil error while not at the end of the stream")
			}

			api.NoContent(w)
			return
		}
	}
}
