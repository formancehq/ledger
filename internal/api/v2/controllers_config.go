package v2

import (
	_ "embed"
	"net/http"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/v2/internal/api/backend"
)

type ConfigInfo struct {
	Server  string `json:"server"`
	Version string `json:"version"`
}

func getInfo(backend backend.Backend) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sharedapi.RawOk(w, ConfigInfo{
			Server:  "ledger",
			Version: backend.GetVersion(),
		})
	}
}
