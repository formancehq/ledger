package idempotency

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedlogging"
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
	if _, err := c.Writer.WriteString(r.Body); err != nil {
		sharedlogging.GetLogger(c.Request.Context()).Errorf("Error writing stored response: %s", err)
	}
}
