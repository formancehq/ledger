package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/formancehq/go-libs/v5/pkg/transport/api"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func importLogs(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	stream := make(chan ledger.Log)
	errChan := make(chan error, 1)
	go func() {
		err := common.LedgerFromContext(ctx).Import(ctx, stream)
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
				// Closing alone means successful EOF to the importer. Cancel first
				// so a malformed suffix cannot commit a valid prefix.
				cancel()
				close(stream)
				_ = <-errChan
				common.InternalServerError(w, r, fmt.Errorf("reading input stream: %w", err))
				return
			}
		}

		select {
		case stream <- l:
		case <-r.Context().Done():
			cancel()
			close(stream)
			common.InternalServerError(w, r, fmt.Errorf("request context done: %w", r.Context().Err()))
			return
		case err := <-errChan:
			close(stream)
			if err != nil {
				handleError(err)
				return
			}
			panic("got nil error while not at the end of the stream")
		}
	}
}
