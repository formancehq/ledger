package controllers

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/health"
)

type HealthController struct {
	Checks []health.NamedCheck
}

type result struct {
	Check health.NamedCheck
	Err   error
}

func (ctrl *HealthController) Check(c *gin.Context) {
	w := sync.WaitGroup{}
	w.Add(len(ctrl.Checks))

	results := make(chan result, len(ctrl.Checks))
	for _, ch := range ctrl.Checks {
		go func(ch health.NamedCheck) {
			defer w.Done()
			select {
			case <-c.Request.Context().Done():
				return
			case results <- result{
				Check: ch,
				Err:   ch.Do(c.Request.Context()),
			}:
			}
		}(ch)
	}
	w.Wait()
	close(results)

	response := map[string]string{}
	hasError := false
	for r := range results {
		if r.Err != nil {
			hasError = true
			response[r.Check.Name()] = r.Err.Error()
		} else {
			response[r.Check.Name()] = "OK"
		}
	}

	if hasError {
		c.JSON(http.StatusInternalServerError, response)
	} else {
		c.JSON(http.StatusOK, response)
	}
}

func NewHealthController(checks []health.NamedCheck) HealthController {
	return HealthController{
		Checks: checks,
	}
}
