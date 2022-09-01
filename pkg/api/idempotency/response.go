package idempotency

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Response struct {
	RequestHash string
	StatusCode  int
	Header      http.Header
	Body        string
}

func (r Response) write(c *gin.Context) {
	for k, v := range r.Header {
		for _, vv := range v {
			c.Writer.Header().Add(k, vv)
		}
	}
	c.Writer.WriteHeader(r.StatusCode)
	c.Writer.WriteString(r.Body)
}
