package api

import (
	"net/http"
)

type healthRepository interface {
	Ping() error
}

func healthHandler(repo healthRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := repo.Ping(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func liveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
}
