package v1

import (
	_ "embed"
	"net/http"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/collectionutils"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

type ConfigInfo struct {
	Server  string        `json:"server"`
	Version string        `json:"version"`
	Config  *LedgerConfig `json:"config"`
}

type LedgerConfig struct {
	LedgerStorage         *LedgerStorage                         `json:"storage"`
	SchemaEnforcementMode ledgercontroller.SchemaEnforcementMode `json:"schemaEnforcementMode"`
}

type LedgerStorage struct {
	Driver  string   `json:"driver"`
	Ledgers []string `json:"ledgers"`
}

func GetInfo(systemController system.Controller, version string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		ledgerNames := make([]string, 0)
		if err := storagecommon.Iterate(r.Context(), storagecommon.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
			PageSize: 100,
		},
			systemController.ListLedgers,
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
				SchemaEnforcementMode: systemController.GetSchemaEnforcementMode(r.Context()),
				LedgerStorage: &LedgerStorage{
					Driver:  "postgres",
					Ledgers: ledgerNames,
				},
			},
		})
	}
}
