package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	auth "github.com/formancehq/auth/pkg"
	"github.com/formancehq/auth/pkg/storage/sqlstorage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func withDbAndScopesRouter(t *testing.T, callback func(router *mux.Router, db *gorm.DB)) {
	db, err := sqlstorage.LoadGorm(sqlite.Open(":memory:"), testing.Verbose())
	require.NoError(t, err)
	require.NoError(t, sqlstorage.MigrateTables(context.Background(), db))

	router := mux.NewRouter()
	addScopeRoutes(db, router)

	callback(router, db)
}

func TestCreateScope(t *testing.T) {
	withDbAndScopesRouter(t, func(router *mux.Router, db *gorm.DB) {
		req := httptest.NewRequest(http.MethodPost, "/scopes", createJSONBuffer(t, auth.ScopeOptions{
			Label: "XXX",
		}))
		res := httptest.NewRecorder()

		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusCreated, res.Code)

		createdScope := readTestResponse[scope](t, res)
		require.NotEmpty(t, createdScope.ID)
		require.Equal(t, "XXX", createdScope.Label)

		scopeFromDatabase := auth.Scope{}
		require.NoError(t, db.Find(&scopeFromDatabase, "id = ?", createdScope.ID).Error)
		require.Equal(t, scopeFromDatabase.Label, createdScope.Label)
	})
}

func TestUpdateScope(t *testing.T) {
	withDbAndScopesRouter(t, func(router *mux.Router, db *gorm.DB) {

		scope1 := auth.NewScope(auth.ScopeOptions{Label: "XXX"})
		require.NoError(t, db.Create(scope1).Error)

		req := httptest.NewRequest(http.MethodPut, "/scopes/"+scope1.ID, createJSONBuffer(t, auth.ScopeOptions{
			Label: "YYY",
		}))
		res := httptest.NewRecorder()

		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)

		updatedScope := readTestResponse[scope](t, res)
		require.NotEmpty(t, updatedScope.ID)
		require.Equal(t, "YYY", updatedScope.Label)

		scopeFromDatabase := auth.Scope{}
		require.NoError(t, db.Find(&scopeFromDatabase, "id = ?", updatedScope.ID).Error)
		require.Equal(t, scopeFromDatabase.Label, updatedScope.Label)
	})
}

func TestListScopes(t *testing.T) {
	withDbAndScopesRouter(t, func(router *mux.Router, db *gorm.DB) {
		scope1 := auth.NewScope(auth.ScopeOptions{Label: "XXX"})
		require.NoError(t, db.Create(scope1).Error)

		scope2 := auth.NewScope(auth.ScopeOptions{Label: "YYY"}).AddTransientScope(scope1)
		require.NoError(t, db.Create(scope2).Error)

		req := httptest.NewRequest(http.MethodGet, "/scopes", nil)
		res := httptest.NewRecorder()

		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)

		scopes := readTestResponse[[]scope](t, res)
		require.Len(t, scopes, 2)
		require.Len(t, scopes[1].Transient, 1)
		require.Equal(t, scopes[1].Transient[0], scopes[0].ID)
	})
}

func TestReadScope(t *testing.T) {
	withDbAndScopesRouter(t, func(router *mux.Router, db *gorm.DB) {
		scope1 := auth.NewScope(auth.ScopeOptions{Label: "XXX"})
		require.NoError(t, db.Create(scope1).Error)

		scope2 := auth.NewScope(auth.ScopeOptions{Label: "YYY"}).AddTransientScope(scope1)
		require.NoError(t, db.Create(scope2).Error)

		req := httptest.NewRequest(http.MethodGet, "/scopes/"+scope2.ID, nil)
		res := httptest.NewRecorder()

		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)

		scope := readTestResponse[scope](t, res)
		require.Len(t, scope.Transient, 1)
		require.Equal(t, scope.Transient[0], scope1.ID)
	})
}

func TestDeleteScope(t *testing.T) {
	withDbAndScopesRouter(t, func(router *mux.Router, db *gorm.DB) {
		scope1 := auth.NewScope(auth.ScopeOptions{Label: "XXX"})
		require.NoError(t, db.Create(scope1).Error)

		req := httptest.NewRequest(http.MethodDelete, "/scopes/"+scope1.ID, nil)
		res := httptest.NewRecorder()

		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusNoContent, res.Code)
	})
}

func TestAddTransientScope(t *testing.T) {
	withDbAndScopesRouter(t, func(router *mux.Router, db *gorm.DB) {
		scope1 := auth.NewScope(auth.ScopeOptions{Label: "XXX"})
		require.NoError(t, db.Create(scope1).Error)

		scope2 := auth.NewScope(auth.ScopeOptions{Label: "YYY"})
		require.NoError(t, db.Create(scope2).Error)

		req := httptest.NewRequest(http.MethodPut, "/scopes/"+scope1.ID+"/transient/"+scope2.ID, nil)
		res := httptest.NewRecorder()

		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusNoContent, res.Code)

		scopeFromDatabase := auth.Scope{}
		require.NoError(t, db.Preload("TransientScopes").Find(&scopeFromDatabase, "id = ?", scope1.ID).Error)
		require.Equal(t, scopeFromDatabase.Label, scope1.Label)
		require.Len(t, scopeFromDatabase.TransientScopes, 1)
		require.Equal(t, scopeFromDatabase.TransientScopes[0], *scope2)
	})
}

func TestDeleteTransientScope(t *testing.T) {
	withDbAndScopesRouter(t, func(router *mux.Router, db *gorm.DB) {
		scope1 := auth.NewScope(auth.ScopeOptions{Label: "XXX"})
		require.NoError(t, db.Create(scope1).Error)

		scope2 := auth.NewScope(auth.ScopeOptions{Label: "YYY"}).AddTransientScope(scope1)
		require.NoError(t, db.Create(scope2).Error)

		req := httptest.NewRequest(http.MethodDelete, "/scopes/"+scope2.ID+"/transient/"+scope1.ID, nil)
		res := httptest.NewRecorder()

		router.ServeHTTP(res, req)

		require.Equal(t, http.StatusNoContent, res.Code)

		scopeFromDatabase := auth.Scope{}
		require.NoError(t, db.Preload("TransientScopes").Find(&scopeFromDatabase, "id = ?", scope2.ID).Error)
		require.Equal(t, scopeFromDatabase.Label, scope2.Label)
		require.Len(t, scopeFromDatabase.TransientScopes, 0)
	})
}
