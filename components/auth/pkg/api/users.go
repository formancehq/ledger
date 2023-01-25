package api

import (
	"net/http"

	auth "github.com/formancehq/auth/pkg"
	"github.com/gorilla/mux"
	"gorm.io/gorm"
)

func addUserRoutes(db *gorm.DB, router *mux.Router) {
	router.Path("/users").Methods(http.MethodGet).HandlerFunc(listUsers(db))
	router.Path("/users/{userId}").Methods(http.MethodGet).HandlerFunc(readUser(db))
}

func listUsers(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		users := make([]auth.User, 0)
		if err := db.
			WithContext(r.Context()).
			Find(&users).Error; err != nil {
			internalServerError(w, r, err)
			return
		}
		writeJSONObject(w, r, users)
	}
}

func readUser(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := findById[auth.User](w, r, db, "userId")
		if user == nil {
			return
		}
		writeJSONObject(w, r, user)
	}
}
