package raft

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
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
func NewAction(actionType ledgerpb.ActionType, msg proto.Message) *ledgerpb.Action {
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return &ledgerpb.Action{
		ActionType: actionType,
		Data:       data,
	}
}

// NewCommand creates a new Command with the given actions
func NewCommand(actions ...*ledgerpb.Action) *ledgerpb.Command {
	return &ledgerpb.Command{
		Id:      GenerateRandomID(),
		Actions: actions,
		Date:    ledgerpb.NewTimestamp(time.Now()),
	}
}

// NewCreateLedgerCommand creates a new CreateLedgerCommand
// snapshotThreshold is optional: if nil or 0, uses global config
func NewCreateLedgerCommand(cmd *ledgerpb.CreateLedgerCommand) *ledgerpb.Command {
	action := NewAction(ledgerpb.ActionType_CreateLedger, cmd)
	return NewCommand(action)
}

// NewDeleteLedgerCommand creates a new DeleteLedgerCommand
func NewDeleteLedgerCommand(id uint32) *ledgerpb.Command {
	action := NewAction(ledgerpb.ActionType_DeleteLedger, &ledgerpb.DeleteLedgerCommand{
		Id: id,
	})
	return NewCommand(action)
}

// UnmarshalCommandData unmarshals FSM command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *ledgerpb.CreateLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *ledgerpb.DeleteLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *ledgerpb.CreateLogCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}

// NewCreateLogAction creates a new action for creating a log
func NewCreateLogAction(input *ledgerpb.CommandInput, ledgerID uint32, idempotency *ledgerpb.Idempotency) *ledgerpb.Action {
	return NewAction(ledgerpb.ActionType_CreateLog, &ledgerpb.CreateLogCommand{
		Input:       input,
		Idempotency: idempotency,
		LedgerId:    ledgerID,
	})
}

// NewCreateLogCommand creates a new command with a single CreateLog action
func NewCreateLogCommand(input *ledgerpb.CommandInput, ledgerID uint32, idempotency *ledgerpb.Idempotency) *ledgerpb.Command {
	return NewCommand(NewCreateLogAction(input, ledgerID, idempotency))
}
