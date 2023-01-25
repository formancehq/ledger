package api

import (
	"net/http"

	auth "github.com/formancehq/auth/pkg"
	"github.com/gorilla/mux"
	"gorm.io/gorm"
)

func addScopeRoutes(db *gorm.DB, router *mux.Router) {
	router.Path("/scopes").Methods(http.MethodPost).HandlerFunc(createScope(db))
	router.Path("/scopes").Methods(http.MethodGet).HandlerFunc(listScopes(db))
	router.Path("/scopes/{scopeId}").Methods(http.MethodPut).HandlerFunc(updateScope(db))
	router.Path("/scopes/{scopeId}").Methods(http.MethodGet).HandlerFunc(readScope(db))
	router.Path("/scopes/{scopeId}").Methods(http.MethodDelete).HandlerFunc(deleteScope(db))
	router.Path("/scopes/{scopeId}/transient/{transientScopeId}").Methods(http.MethodPut).HandlerFunc(addTriggerToScope(db))
	router.Path("/scopes/{scopeId}/transient/{transientScopeId}").Methods(http.MethodDelete).HandlerFunc(deleteTriggerFromScope(db))
}

type scope struct {
	ID        string   `json:"id"`
	Label     string   `json:"label"`
	Transient []string `json:"transient"`
}

func mapBusinessScope(businessScope auth.Scope) scope {
	return scope{
		ID:    businessScope.ID,
		Label: businessScope.Label,
		Transient: func() []string {
			ret := make([]string, 0)
			for _, trigger := range businessScope.TransientScopes {
				ret = append(ret, trigger.ID)
			}
			return ret
		}(),
	}
}

func deleteTriggerFromScope(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scope := findById[auth.Scope](w, r, db, "scopeId")
		if scope == nil {
			return
		}
		triggered := findById[auth.Scope](w, r, db, "transientScopeId")
		if triggered == nil {
			return
		}
		if err := loadAssociation(w, r, db, scope, "TransientScopes", &scope.TransientScopes); err != nil {
			return
		}
		for _, t := range scope.TransientScopes {
			if t.ID == triggered.ID {
				if err := removeFromAssociation(w, r, db, scope, "TransientScopes", triggered); err != nil {
					return
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}
}

func addTriggerToScope(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scope := findById[auth.Scope](w, r, db, "scopeId")
		if scope == nil {
			return
		}
		triggered := findById[auth.Scope](w, r, db, "transientScopeId")
		if triggered == nil {
			return
		}
		if err := appendToAssociation(w, r, db, scope, "TransientScopes", triggered); err != nil {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func readScope(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scope := findById[auth.Scope](w, r, db, "scopeId")
		if scope == nil {
			return
		}
		if err := loadAssociation(w, r, db, scope, "TransientScopes", &scope.TransientScopes); err != nil {
			return
		}
		writeJSONObject(w, r, mapBusinessScope(*scope))
	}
}

func updateScope(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scope := findById[auth.Scope](w, r, db, "scopeId")
		if scope == nil {
			return
		}
		opts := readJSONObject[auth.ScopeOptions](w, r)
		if opts == nil {
			return
		}
		scope.Update(*opts)

		if err := saveObject(w, r, db, scope); err != nil {
			return
		}
		writeJSONObject(w, r, scope)
	}
}

func deleteScope(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := db.
			WithContext(r.Context()).
			Delete(&auth.Scope{}, "id = ?", mux.Vars(r)["scopeId"]).
			Error
		if err != nil {
			internalServerError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func listScopes(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scopes := make([]auth.Scope, 0)
		if err := db.
			WithContext(r.Context()).
			Preload("TransientScopes").
			Find(&scopes).Error; err != nil {
			internalServerError(w, r, err)
			return
		}
		writeJSONObject(w, r, mapList(scopes, mapBusinessScope))
	}
}

func createScope(db *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		opts := readJSONObject[auth.ScopeOptions](w, r)
		if opts == nil {
			return
		}
		scope := auth.NewScope(*opts)
		if err := createObject(w, r, db, scope); err != nil {
			return
		}
		writeCreatedJSONObject(w, r, scope, scope.ID)
	}
}
