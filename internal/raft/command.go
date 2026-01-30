package raft

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// GenerateRandomID generates a random uint64 ID
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

// NewCommandBatch creates a new CommandBatch with the given actions
func NewCommandBatch(actions ...*raftcmdpb.Action) *raftcmdpb.CommandBatch {
	return &raftcmdpb.CommandBatch{
		Id:      GenerateRandomID(),
		Actions: actions,
		Date:    commonpb.NewTimestamp(time.Now()),
	}
}

// NewCreateLedgerAction creates an action for creating a ledger
func NewCreateLedgerAction(cmd *raftcmdpb.CreateLedgerCommand) *raftcmdpb.Action {
	return &raftcmdpb.Action{
		Command: &raftcmdpb.AnyCommand{
			Command: &raftcmdpb.AnyCommand_CreateLedger{
				CreateLedger: cmd,
			},
		},
	}
}

// NewDeleteLedgerAction creates an action for deleting a ledger
func NewDeleteLedgerAction(cmd *raftcmdpb.DeleteLedgerCommand) *raftcmdpb.Action {
	return &raftcmdpb.Action{
		Command: &raftcmdpb.AnyCommand{
			Command: &raftcmdpb.AnyCommand_DeleteLedger{
				DeleteLedger: cmd,
			},
		},
	}
}

// NewCreateLedgerLogAction creates an action for creating a ledger log
func NewCreateLedgerLogAction(cmd *raftcmdpb.CreateLedgerLogCommand) *raftcmdpb.Action {
	return &raftcmdpb.Action{
		Command: &raftcmdpb.AnyCommand{
			Command: &raftcmdpb.AnyCommand_CreateLedgerLog{
				CreateLedgerLog: cmd,
			},
		},
	}
}
