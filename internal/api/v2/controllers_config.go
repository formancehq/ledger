package v2

import (
	_ "embed"
	"net/http"

	"github.com/formancehq/ledger/internal/api/backend"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
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
