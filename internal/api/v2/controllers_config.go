package v2

import (
	_ "embed"
	"net/http"

	"github.com/formancehq/go-libs/api"
)

type ConfigInfo struct {
	Server  string `json:"server"`
	Version string `json:"version"`
}

func getInfo(version string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		api.RawOk(w, ConfigInfo{
			Server:  "ledger",
			Version: version,
		})
	}
}
