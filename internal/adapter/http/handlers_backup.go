package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type backupBody struct {
	BucketID string             `json:"bucketId"`
	Storage  *backupStorageBody `json:"storage"`
}

// handleBackup handles POST /_/cluster/backups. The call returns once the
// checkpoint is published or the backup fails; a client disconnect cancels it.
func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	var body backupBody
	if !decodeClusterBody(w, r, &body) {
		return
	}
	storage, err := body.Storage.toProto()
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	resp, err := s.backend.Backup(r.Context(), &clusterpb.BackupRequest{
		BucketId: body.BucketID,
		Storage:  storage,
	})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	writeOK(w, r, resp)
}
