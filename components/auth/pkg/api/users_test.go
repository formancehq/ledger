package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	auth "github.com/formancehq/auth/pkg"
	"github.com/formancehq/auth/pkg/storage/sqlstorage"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	user1 = auth.User{
		ID:      uuid.NewString(),
		Subject: "alice",
		Email:   "alice@formance.com",
	}

	user2 = auth.User{
		ID:      uuid.NewString(),
		Subject: "bob",
		Email:   "bob@formance.com",
	}
)

func TestListUsers(t *testing.T) {
	withDbAndUserRouter(t, func(router *mux.Router, db *gorm.DB) {
		require.NoError(t, db.Create(user1).Error)
		require.NoError(t, db.Create(user2).Error)

		req := httptest.NewRequest(http.MethodGet, "/users", nil)
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		require.Equal(t, http.StatusOK, res.Code)

		users := readTestResponse[[]auth.User](t, res)
		require.Len(t, users, 2)
	})
}

func TestReadUser(t *testing.T) {
	withDbAndUserRouter(t, func(router *mux.Router, db *gorm.DB) {
		require.NoError(t, db.Create(user1).Error)

		req := httptest.NewRequest(http.MethodGet, "/users/"+user1.ID, nil)
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		require.Equal(t, http.StatusOK, res.Code)

		user := readTestResponse[auth.User](t, res)
		require.Equal(t, user1, user)
	})
}

func withDbAndUserRouter(t *testing.T, callback func(router *mux.Router, db *gorm.DB)) {
	db, err := sqlstorage.LoadGorm(sqlite.Open(":memory:"), testing.Verbose())
	require.NoError(t, err)
	require.NoError(t, sqlstorage.MigrateTables(context.Background(), db))

	router := mux.NewRouter()
	addUserRoutes(db, router)

	callback(router, db)
}
