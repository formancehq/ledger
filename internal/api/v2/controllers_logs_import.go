package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/formancehq/go-libs/v4/api"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
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
		default:
			common.HandleCommonErrors(w, r, err)
		}
	}
	for {
		l := ledger.Log{}
		if err := dec.Decode(&l); err != nil {
			if errors.Is(err, io.EOF) {
				close(stream)
				// Block on the import goroutine's result — it is
				// authoritative once the stream is closed.
				if err := <-errChan; err != nil {
					handleError(err)
					return
				}
				api.NoContent(w)
				return
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
			panic("got nil error while not at the end of the stream")
		}
	}
}
