package idempotency

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/errors"
	"github.com/numary/ledger/pkg/storage"
)

const (
	HeaderIdempotency = "Idempotency-Key"
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

		// Store created
		if err != storage.ErrLedgerStoreNotFound {
			response, err := store.ReadIK(c.Request.Context(), ik)
			if err != nil && err != ErrIKNotFound {
				errors.ResponseError(c, err)
				return
			}
			if err == nil {
				if hashRequest(c.Request) != response.RequestHash {
					c.AbortWithStatus(400)
					return
				}

				c.Abort()
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
				RequestHash: hashRequest(c.Request),
				StatusCode:  c.Writer.Status(),
				Header:      c.Writer.Header(),
				Body:        string(rw.Bytes()),
			}); err != nil {
				panic(err)
			}
		}
	}
}
