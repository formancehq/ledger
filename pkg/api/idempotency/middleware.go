package idempotency

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

const (
	HeaderIdempotency    = "Idempotency-Key"
	HeaderIdempotencyHit = "Idempotency-Hit"
)

func Middleware(driver storage.LedgerStoreProvider[Store]) func(c *gin.Context) {
	return func(c *gin.Context) {

		ik := c.Request.Header.Get(HeaderIdempotency)
		if ik == "" {
			return
		}

		// Do not create the store if it doesn't exist
		store, _, err := driver.GetLedgerStore(c.Request.Context(), c.Param("ledger"), false)
		if err != nil && err != storage.ErrLedgerStoreNotFound {
			apierrors.ResponseError(c, err)
			return
		}

		data, err := io.ReadAll(c.Request.Body)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(data))

		// Store created
		if store != nil {
			response, err := store.ReadIK(c.Request.Context(), ik)
			if err != nil && err != ErrIKNotFound {
				apierrors.ResponseError(c, err)
				return
			}
			if err == nil {
				if hashRequest(c.Request.URL.String(), string(data)) != response.RequestHash {
					c.AbortWithStatus(http.StatusBadRequest)
					return
				}

				c.Abort()
				c.Writer.Header().Set(HeaderIdempotencyHit, "true")
				response.write(c)
				return
			}
		}

		rw := newResponseWriter(c.Writer)
		c.Writer = rw

		c.Next()
		if c.Writer.Status() >= 200 && c.Writer.Status() < 300 {
			if store == nil {
				store, _, err = driver.GetLedgerStore(c.Request.Context(), c.Param("ledger"), true)
				if err != nil {
					_ = c.Error(errors.Wrap(err, "retrieving ledger store to save IK"))
					return
				}
			}
			if err := store.CreateIK(c.Request.Context(), ik, Response{
				RequestHash: hashRequest(c.Request.URL.String(), string(data)),
				StatusCode:  c.Writer.Status(),
				Header:      c.Writer.Header(),
				// TODO: Check if PG accept big documents
				Body: string(rw.Bytes()),
			}); err != nil {
				_ = c.Error(errors.Wrap(err, "persisting IK to database"))
			}
		}
	}
}
