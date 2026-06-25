package http

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

// infoResponse is the flat JSON body of GET /_info (camelCase, unwrapped to
// match the platform _info convention).
type infoResponse struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

// infoHandler returns an unauthenticated handler reporting the server build
// metadata. The payload is written directly (not via writeOK) so it is not
// wrapped in BaseResponse, matching the issue spec and the _info convention.
func infoHandler(info version.Info) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// best-effort: status 200 + Content-Type are already written, so there
		// is nothing actionable to do on an encode failure of this static payload.
		_ = json.NewEncoder(w).Encode(infoResponse{
			Version:   info.Version,
			Commit:    info.Commit,
			BuildDate: info.BuildDate,
			GoVersion: info.GoVersion,
		})
	}
}
