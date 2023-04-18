package health

import (
	"encoding/json"
	"net/http"
	"sync"
)

type HealthController struct {
	Checks []NamedCheck
}

type result struct {
	Check NamedCheck
	Err   error
}

func (ctrl *HealthController) Check(w http.ResponseWriter, r *http.Request) {
	sg := sync.WaitGroup{}
	sg.Add(len(ctrl.Checks))

	results := make(chan result, len(ctrl.Checks))
	for _, ch := range ctrl.Checks {
		go func(ch NamedCheck) {
			defer sg.Done()
			select {
			case <-r.Context().Done():
				return
			case results <- result{
				Check: ch,
				Err:   ch.Do(r.Context()),
			}:
			}
		}(ch)
	}
	sg.Wait()
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
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		panic(err)
	}
}

func NewHealthController(checks []NamedCheck) *HealthController {
	return &HealthController{
		Checks: checks,
	}
}
