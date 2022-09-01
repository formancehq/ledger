package idempotency

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/storage"
	"github.com/stretchr/testify/require"
)

func init() {
	gin.SetMode(gin.ReleaseMode)
}

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

		handler := gin.New()
		handler.GET("/", Middleware(storeProvider), func(c *gin.Context) {
			if called {
				c.Writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			called = true
			c.Writer.WriteHeader(http.StatusAccepted)
			c.Writer.Write([]byte(body))
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

		handler := gin.New()
		handler.GET("/", Middleware(storeProvider), func(c *gin.Context) {
			if called {
				// Simulate the store creation by a service
				c.Writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			called = true
			store = NewInMemoryStore()
			c.Writer.WriteHeader(http.StatusAccepted)
			c.Writer.Write([]byte(body))
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

		handler := gin.New()
		handler.GET("/", Middleware(storeProvider), func(c *gin.Context) {
			c.Writer.WriteHeader(http.StatusServiceUnavailable)
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

		handler := gin.New()
		handler.GET("/path1", Middleware(storeProvider), func(c *gin.Context) {
			c.Writer.WriteHeader(http.StatusAccepted)
		})
		handler.GET("/path2", Middleware(storeProvider), func(c *gin.Context) {
			c.Writer.WriteHeader(http.StatusAccepted)
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

		handler := gin.New()
		handler.GET("/", Middleware(storeProvider), func(c *gin.Context) {
			data, err := io.ReadAll(c.Request.Body)
			require.NoError(t, err)
			require.Equal(t, requestBody, string(data))
			c.Writer.WriteHeader(http.StatusNoContent)
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
