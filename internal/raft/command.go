package raft

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"google.golang.org/protobuf/proto"
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

// NewAction creates a new Action with the given type and data
func NewAction(actionType raftcmdpb.ActionType, msg proto.Message) *raftcmdpb.Action {
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return &raftcmdpb.Action{
		ActionType: actionType,
		Data:       data,
	}
}

// NewCommand creates a new Command with the given actions
func NewCommand(actions ...*raftcmdpb.Action) *raftcmdpb.Command {
	return &raftcmdpb.Command{
		Id:      GenerateRandomID(),
		Actions: actions,
		Date:    commonpb.NewTimestamp(time.Now()),
	}
}

// NewCreateLedgerCommand creates a new CreateLedgerCommand
// snapshotThreshold is optional: if nil or 0, uses global config
func NewCreateLedgerCommand(cmd *raftcmdpb.CreateLedgerCommand) *raftcmdpb.Command {
	action := NewAction(raftcmdpb.ActionType_CreateLedger, cmd)
	return NewCommand(action)
}

// NewDeleteLedgerCommand creates a new DeleteLedgerCommand
func NewDeleteLedgerCommand(id uint32) *raftcmdpb.Command {
	action := NewAction(raftcmdpb.ActionType_DeleteLedger, &raftcmdpb.DeleteLedgerCommand{
		Id: id,
	})
	return NewCommand(action)
}

// UnmarshalCommandData unmarshals FSM command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *raftcmdpb.CreateLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *raftcmdpb.DeleteLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *raftcmdpb.CreateLogCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}

// NewCreateLogAction creates a new action for creating a log
func NewCreateLogAction(input *raftcmdpb.CommandInput, ledgerID uint32, idempotency *commonpb.Idempotency) *raftcmdpb.Action {
	return NewAction(raftcmdpb.ActionType_CreateLog, &raftcmdpb.CreateLogCommand{
		Input:       input,
		Idempotency: idempotency,
		LedgerId:    ledgerID,
	})
}

// NewCreateLogCommand creates a new command with a single CreateLog action
func NewCreateLogCommand(input *raftcmdpb.CommandInput, ledgerID uint32, idempotency *commonpb.Idempotency) *raftcmdpb.Command {
	return NewCommand(NewCreateLogAction(input, ledgerID, idempotency))
}
