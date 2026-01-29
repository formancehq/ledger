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

// NewCommandBatch creates a new CommandBatch with the given actions
func NewCommandBatch(actions ...*raftcmdpb.Action) *raftcmdpb.CommandBatch {
	return &raftcmdpb.CommandBatch{
		Id:      GenerateRandomID(),
		Actions: actions,
		Date:    commonpb.NewTimestamp(time.Now()),
	}
}

// NewCreateLedgerCommand creates a new CreateLedgerCommand
// snapshotThreshold is optional: if nil or 0, uses global config
func NewCreateLedgerCommand(cmd *raftcmdpb.CreateLedgerCommand) *raftcmdpb.CommandBatch {
	action := NewAction(raftcmdpb.ActionType_CreateLedger, cmd)
	return NewCommandBatch(action)
}

// NewDeleteLedgerCommand creates a new DeleteLedgerCommand
func NewDeleteLedgerCommand(id uint32) *raftcmdpb.CommandBatch {
	action := NewAction(raftcmdpb.ActionType_DeleteLedger, &raftcmdpb.DeleteLedgerCommand{
		Id: id,
	})
	return NewCommandBatch(action)
}

// UnmarshalCommandData unmarshals FSM command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *raftcmdpb.CreateLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *raftcmdpb.DeleteLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *raftcmdpb.CreateLedgerLogCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}

// NewCreateLedgerLogAction creates a new action for creating a ledger log
func NewCreateLedgerLogAction(cmd *raftcmdpb.CreateLedgerLogCommand) *raftcmdpb.Action {
	return NewAction(raftcmdpb.ActionType_CreateLedgerLog, cmd)
}

// NewCreateLedgerLogCommandWrapper creates a new command with a single CreateLedgerLog action
func NewCreateLedgerLogCommandWrapper(cmd *raftcmdpb.CreateLedgerLogCommand) *raftcmdpb.CommandBatch {
	return NewCommandBatch(NewCreateLedgerLogAction(cmd))
}

// NewActionFromData creates a new action from ActionData
func NewActionFromData(data *raftcmdpb.ActionData) *raftcmdpb.Action {
	if data.Command == nil {
		panic("command is nil")
	}
	switch c := data.Command.Command.(type) {
	case *raftcmdpb.AnyCommand_CreateLedger:
		return NewAction(raftcmdpb.ActionType_CreateLedger, c.CreateLedger)
	case *raftcmdpb.AnyCommand_DeleteLedger:
		return NewAction(raftcmdpb.ActionType_DeleteLedger, c.DeleteLedger)
	case *raftcmdpb.AnyCommand_CreateLedgerLog:
		return NewAction(raftcmdpb.ActionType_CreateLedgerLog, c.CreateLedgerLog)
	default:
		panic("unknown command type")
	}
}
