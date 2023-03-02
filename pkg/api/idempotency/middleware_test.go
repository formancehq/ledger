package idempotency

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/numary/ledger/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIdempotency(t *testing.T) {

	var newReqRec = func(ik string) (*http.Request, *httptest.ResponseRecorder) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(HeaderIdempotency, ik)
		return req, rec
	}

	var drainBody = func(rec *httptest.ResponseRecorder) string {
		data, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		return string(data)
	}

	t.Run("With existing store", func(t *testing.T) {
		store := NewInMemoryStore()
		storeProvider := storage.LedgerStoreProviderFn[Store](func(ctx context.Context, name string, create bool) (Store, bool, error) {
			return store, false, nil
		})

		called := false

		body := "hello world!"

		handler := chi.NewMux()
		handler.Use(Middleware(storeProvider))
		handler.Get("/", func(w http.ResponseWriter, r *http.Request) {
			if called {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			called = true
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(body))
		})

		ik := "foo"

		req, rec := newReqRec(ik)

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Result().StatusCode)
		require.Equal(t, body, drainBody(rec))

		req, rec = newReqRec(ik)

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Result().StatusCode)
		require.Equal(t, body, drainBody(rec))
	})

	t.Run("With non existing store", func(t *testing.T) {
		var store *inMemoryStore
		storeProvider := storage.LedgerStoreProviderFn[Store](func(ctx context.Context, name string, create bool) (Store, bool, error) {
			if store == nil {
				return nil, false, storage.ErrLedgerStoreNotFound
			}
			return store, false, nil
		})

		called := false
		body := "Hello world!"

		handler := chi.NewRouter()
		handler.Use(Middleware(storeProvider))
		handler.Get("/", func(w http.ResponseWriter, r *http.Request) {
			if called {
				// Simulate the store creation by a service
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			called = true
			store = NewInMemoryStore()
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(body))
		})

		ik := "foo"

		req, rec := newReqRec(ik)

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Result().StatusCode)
		require.Equal(t, body, drainBody(rec))

		req, rec = newReqRec(ik)

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Result().StatusCode)
		require.Equal(t, body, drainBody(rec))
	})
	t.Run("With error on inner handler", func(t *testing.T) {
		store := NewInMemoryStore()
		storeProvider := storage.LedgerStoreProviderFn[Store](func(ctx context.Context, name string, create bool) (Store, bool, error) {
			return store, false, nil
		})

		handler := chi.NewRouter()
		handler.Use(Middleware(storeProvider))
		handler.Get("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})

		req, rec := newReqRec("foo")

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusServiceUnavailable, rec.Result().StatusCode)
		require.Empty(t, store.iks)
	})
	t.Run("With same IK for two requests", func(t *testing.T) {
		store := NewInMemoryStore()
		storeProvider := storage.LedgerStoreProviderFn[Store](func(ctx context.Context, name string, create bool) (Store, bool, error) {
			return store, false, nil
		})

		handler := chi.NewRouter()
		handler.Use(Middleware(storeProvider))
		handler.Get("/path1", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		})
		handler.Get("/path2", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		})

		ik := "foo"

		req, rec := newReqRec(ik)
		req.URL.Path = "/path1"

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Result().StatusCode)

		req, rec = newReqRec(ik)
		req.URL.Path = "/path2"

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Result().StatusCode)
	})
	t.Run("With request body", func(t *testing.T) {
		store := NewInMemoryStore()
		storeProvider := storage.LedgerStoreProviderFn[Store](func(ctx context.Context, name string, create bool) (Store, bool, error) {
			return store, false, nil
		})

		requestBody := "Hello world!"

		handler := chi.NewRouter()
		handler.Use(Middleware(storeProvider))
		handler.Get("/", func(w http.ResponseWriter, r *http.Request) {
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, requestBody, string(data))
			w.WriteHeader(http.StatusNoContent)
		})

		ik := "foo"

		req, rec := newReqRec(ik)
		req.Body = io.NopCloser(strings.NewReader(requestBody))

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusNoContent, rec.Result().StatusCode)

		req, rec = newReqRec(ik)
		req.Body = io.NopCloser(strings.NewReader(requestBody))

		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusNoContent, rec.Result().StatusCode)
		require.Equal(t, rec.Result().Header.Get(HeaderIdempotencyHit), "true")
	})
}
