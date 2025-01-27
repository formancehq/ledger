package v1

import (
	"context"
	_ "embed"
	"github.com/formancehq/ledger/internal/api/common"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/v2/collectionutils"

	"github.com/formancehq/go-libs/v2/api"
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

func GetInfo(systemController system.Controller, version string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		ledgerNames := make([]string, 0)
		if err := bunpaginate.Iterate(r.Context(), ledgercontroller.NewListLedgersQuery(100),
			func(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
				return systemController.ListLedgers(ctx, q)
			},
			func(cursor *bunpaginate.Cursor[ledger.Ledger]) error {
				ledgerNames = append(ledgerNames, collectionutils.Map(cursor.Data, func(from ledger.Ledger) string {
					return from.Name
				})...)
				return nil
			},
		); err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}

		api.Ok(w, ConfigInfo{
			Server:  "ledger",
			Version: version,
			Config: &LedgerConfig{
				LedgerStorage: &LedgerStorage{
					Driver:  "postgres",
					Ledgers: ledgerNames,
				},
			},
		})
	}
}
