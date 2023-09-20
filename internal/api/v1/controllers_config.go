package v1

import (
	_ "embed"
	"net/http"

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
		ledgers, err := backend.ListLedgers(r.Context())
		if err != nil {
			panic(err)
		}

		sharedapi.Ok(w, ConfigInfo{
			Server:  "ledger",
			Version: backend.GetVersion(),
			Config: &LedgerConfig{
				LedgerStorage: &LedgerStorage{
					Driver:  "postgres",
					Ledgers: ledgers,
				},
			},
		})
	}
}
