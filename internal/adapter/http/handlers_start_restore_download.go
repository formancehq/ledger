package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

type startRestoreDownloadBody struct {
	BucketID string             `json:"bucketId"`
	Storage  *backupStorageBody `json:"storage"`
}

// handleStartRestoreDownload handles POST /_/restore/downloads. The download
// job belongs to the restore process, not to this request. The route is
// unauthenticated, so a custom storage endpoint must carry its own keys.
func (s *restoreServer) handleStartRestoreDownload(w http.ResponseWriter, r *http.Request) {
	var body startRestoreDownloadBody
	if !decodeClusterBody(w, r, &body) {
		return
	}
	storage, err := body.Storage.toProto()
	if err == nil {
		err = body.Storage.requireEndpointCredentials()
	}
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	resp, err := s.backend.StartDownloadBackup(r.Context(), &restorepb.StartDownloadBackupRequest{
		BucketId: body.BucketID,
		Storage:  storage,
	})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	w.Header().Set("Location", APIVersionPrefix+"/_/restore/downloads/"+resp.GetJobId())
	writeJSONResponse(w, r, http.StatusAccepted, BaseResponse[any]{Data: resp})
}
