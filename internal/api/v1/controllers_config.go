package v1

import (
	"context"
	_ "embed"
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"

	"github.com/formancehq/ledger/internal/api/backend"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
)

type ConfigInfo struct {
	Server  string        `json:"server"`
	Version string        `json:"version"`
	Config  *LedgerConfig `json:"config"`
}

type LedgerConfig struct {
	LedgerStorage *LedgerStorage `json:"storage"`
}

type LedgerStorage struct {
	Driver  string   `json:"driver"`
	Ledgers []string `json:"ledgers"`
}

func getInfo(backend backend.Backend) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		ledgerNames := make([]string, 0)
		if err := bunpaginate.Iterate(r.Context(), systemstore.NewListLedgersQuery(100),
			func(ctx context.Context, q systemstore.ListLedgersQuery) (*sharedapi.Cursor[systemstore.Ledger], error) {
				return backend.ListLedgers(ctx, q)
			},
			func(cursor *sharedapi.Cursor[systemstore.Ledger]) error {
				ledgerNames = append(ledgerNames, collectionutils.Map(cursor.Data, func(from systemstore.Ledger) string {
					return from.Name
				})...)
				return nil
			},
		); err != nil {
			sharedapi.InternalServerError(w, r, err)
			return
		}

		sharedapi.Ok(w, ConfigInfo{
			Server:  "ledger",
			Version: backend.GetVersion(),
			Config: &LedgerConfig{
				LedgerStorage: &LedgerStorage{
					Driver:  "postgres",
					Ledgers: ledgerNames,
				},
			},
		})
	}
}
