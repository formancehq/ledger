package commands

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// GenerateRandomID generates a random uint64 ID.
func GenerateRandomID() uint64 {
	var b [8]byte

	_, err := rand.Read(b[:])
	if err != nil {
		// Fallback to a simple random number if crypto/rand fails
		// This should never happen in practice
		return uint64(time.Now().UnixNano())
	}

	return binary.BigEndian.Uint64(b[:])
}

// NewCommand creates a new Proposal with the given orders.
func NewCommand(orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
	cmd := &raftcmdpb.Proposal{
		Id:            GenerateRandomID(),
		Date:          commonpb.NewTimestamp(time.Now()),
		ExecutionPlan: &raftcmdpb.ExecutionPlan{},
	}
	if len(orders) > 0 {
		cmd.Payload = &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: orders}}
	}

	return cmd
}

// NewTechnicalCommand creates an execution batch for technical updates.
func NewTechnicalCommand(updates ...*raftcmdpb.TechnicalUpdate) *raftcmdpb.Proposal {
	cmd := NewCommand()
	cmd.Payload = &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{TechnicalUpdates: updates}}

	return cmd
}
