package idempotency

import (
	"bytes"
	"io"
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/storage"
)

const (
	HeaderIdempotency    = "Idempotency-Key"
	HeaderIdempotencyHit = "Idempotency-Hit"
)

func Middleware(driver storage.LedgerStoreProvider[Store]) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ik := r.Header.Get(HeaderIdempotency)
			if ik == "" {
				handler.ServeHTTP(w, r)
				return
			}

			// Do not create the store if it doesn't exist
			store, _, err := driver.GetLedgerStore(r.Context(), chi.URLParam(r, "ledger"), false)
			if err != nil && err != storage.ErrLedgerStoreNotFound {
				apierrors.ResponseError(w, r, err)
				return
			}

			data, err := io.ReadAll(r.Body)
			if err != nil {
				apierrors.ResponseError(w, r, err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))

			// Store created
			if store != nil {
				response, err := store.ReadIK(r.Context(), ik)
				if err != nil && err != ErrIKNotFound {
					apierrors.ResponseError(w, r, err)
					return
				}
				if err == nil {
					if hashRequest(r.URL.String(), string(data)) != response.RequestHash {
						w.WriteHeader(http.StatusBadRequest)
						return
					}

					w.Header().Set(HeaderIdempotencyHit, "true")
					response.write(w, r)
					return
				}
			}

			rw := newResponseWriter(w)
			handler.ServeHTTP(rw, r)

			if rw.statusCode >= 200 && rw.statusCode < 300 {
				if store == nil {
					store, _, err = driver.GetLedgerStore(r.Context(), chi.URLParam(r, "ledger"), true)
					if err != nil {
						logging.FromContext(r.Context()).Errorf("retrieving ledger store to save IK: %s", err)
						return
					}
				}
				if err := store.CreateIK(r.Context(), ik, Response{
					RequestHash: hashRequest(r.URL.String(), string(data)),
					StatusCode:  rw.statusCode,
					Header:      w.Header(),
					// TODO: Check if PG accept big documents
					Body: string(rw.Bytes()),
				}); err != nil {
					logging.FromContext(r.Context()).Errorf("persisting IK to database: %s", err)
					return
				}
			}
		})
	}
}
