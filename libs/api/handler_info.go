package api

import (
	"encoding/json"
	"net/http"
)

type ServiceInfo struct {
	Version string `json:"version"`
}

func InfoHandler(info ServiceInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(info); err != nil {
			panic(err)
		}
	}
}
