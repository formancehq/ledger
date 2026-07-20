package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// numscriptVersionEntryDTO renders one stored version for the REST API.
// createdAt reuses the proto Timestamp, whose MarshalJSON emits RFC3339.
type numscriptVersionEntryDTO struct {
	Version   string              `json:"version"`
	CreatedAt *commonpb.Timestamp `json:"createdAt,omitempty"`
}

// numscriptVersionsDTO is the numscript history response: the current latest
// (greatest stored semver) and every stored version, highest-first.
type numscriptVersionsDTO struct {
	LatestVersion string                     `json:"latestVersion"`
	Versions      []numscriptVersionEntryDTO `json:"versions"`
}

// handleListNumscriptVersions handles GET /{ledgerName}/numscripts/{name}/versions
// to list the numscript's history.
func (s *Server) handleListNumscriptVersions(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))

		return
	}

	latest, versions, err := s.backend.ListNumscriptVersions(r.Context(), ledgerName, name)
	if err != nil {
		handleError(w, r, err)

		return
	}

	dto := numscriptVersionsDTO{LatestVersion: latest, Versions: make([]numscriptVersionEntryDTO, len(versions))}
	for i, v := range versions {
		dto.Versions[i] = numscriptVersionEntryDTO{Version: v.GetVersion(), CreatedAt: v.GetCreatedAt()}
	}

	writeOK(w, dto)
}
