package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/health"
	"net/http"
	"sync"
)

type HealthController struct {
	Checks []health.NamedCheck
}

func (ctrl *HealthController) Check(c *gin.Context) {
	w := sync.WaitGroup{}
	w.Add(len(ctrl.Checks))
	type R struct {
		Check health.NamedCheck
		Err   error
	}
	results := make(chan R, len(ctrl.Checks))
	for _, ch := range ctrl.Checks {
		go func(ch health.NamedCheck) {
			defer w.Done()
			select {
			case <-c.Request.Context().Done():
				return
			case results <- R{
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
	status := http.StatusOK
	if hasError {
		status = http.StatusInternalServerError
	}
	c.JSON(status, response)
}

func NewHealthController(checks []health.NamedCheck) HealthController {
	return HealthController{
		Checks: checks,
	}
}
