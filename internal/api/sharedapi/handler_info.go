package api

import (
	"encoding/json"
	"net/http"
)

type ServiceInfo struct {
	Version string `json:"version"`
	Debug   bool   `json:"debug"`
}

func InfoHandler(info ServiceInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(info); err != nil {
			panic(err)
		}
	}
}
