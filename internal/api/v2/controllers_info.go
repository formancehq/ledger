package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/migrations"
	"github.com/formancehq/ledger/v2/internal/api/backend"
)

type Info struct {
	Name    string      `json:"name"`
	Storage StorageInfo `json:"storage"`
}

type StorageInfo struct {
	Migrations []migrations.Info `json:"migrations"`
}

func getLedgerInfo(w http.ResponseWriter, r *http.Request) {
	ledger := backend.LedgerFromContext(r.Context())

	var err error
	res := Info{
		Name:    chi.URLParam(r, "ledger"),
		Storage: StorageInfo{},
	}
	res.Storage.Migrations, err = ledger.GetMigrationsInfo(r.Context())
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func getStats(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	stats, err := l.Stats(r.Context())
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, stats)
}
