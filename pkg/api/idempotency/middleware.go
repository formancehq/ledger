package idempotency

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/errors"
	"github.com/numary/ledger/pkg/storage"
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
			errors.ResponseError(c, err)
			return
		}

		data, err := io.ReadAll(c.Request.Body)
		if err != nil {
			errors.ResponseError(c, err)
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(data))

		// Store created
		if store != nil {
			response, err := store.ReadIK(c.Request.Context(), ik)
			if err != nil && err != ErrIKNotFound {
				errors.ResponseError(c, err)
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
					errors.ResponseError(c, err)
					return
				}
			}
			if err := store.CreateIK(c.Request.Context(), ik, Response{
				RequestHash: hashRequest(c.Request.URL.String(), string(data)),
				StatusCode:  c.Writer.Status(),
				Header:      c.Writer.Header(),
				Body:        string(rw.Bytes()),
			}); err != nil {
				panic(err)
			}
		}
	}
}
