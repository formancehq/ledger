package http

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	raftmembership "github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type addLearnerBody struct {
	NodeID         *clusterNodeID `json:"nodeId"`
	RaftAddress    string         `json:"raftAddress"`
	ServiceAddress string         `json:"serviceAddress"`
	InstanceID     string         `json:"instanceId"`
	Capabilities   []uint32       `json:"capabilities"`
}

// handleAddLearner handles POST /_/cluster/learners.
func (s *Server) handleAddLearner(w http.ResponseWriter, r *http.Request) {
	var body addLearnerBody
	if !decodeClusterBody(w, r, &body) {
		return
	}
	if body.NodeID == nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("nodeId is required"))

		return
	}
	if body.RaftAddress == "" || body.ServiceAddress == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("raftAddress and serviceAddress are required"))

		return
	}
	if err := raftmembership.ValidateCapabilities(body.Capabilities); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	instanceID, err := uuid.Parse(body.InstanceID)
	if err != nil || len(body.InstanceID) != len(uuid.Nil.String()) {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("instanceId must be a canonical UUID string"))

		return
	}

	_, err = s.backend.AddLearner(r.Context(), &clusterpb.AddLearnerRequest{
		NodeId:         uint64(*body.NodeID),
		RaftAddress:    body.RaftAddress,
		ServiceAddress: body.ServiceAddress,
		InstanceId:     instanceID[:],
		Capabilities:   body.Capabilities,
	})
	if err != nil {
		handleClusterError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
