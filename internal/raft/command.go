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

// NewCreateLedgerCommand creates a new CreateLedgerCommand
// snapshotThreshold is optional: if nil or 0, uses global config
func NewCreateLedgerCommand(cmd *ledgerpb.CreateLedgerCommand) *ledgerpb.Command {

	data, err := proto.Marshal(cmd)
	if err != nil {
		panic(err)
	}

	return &ledgerpb.Command{
		Id:   GenerateRandomID(),
		Type: ledgerpb.CommandType_CreateLedger,
		Data: data,
		Date: ledgerpb.NewTimestamp(time.Now()),
	}
}

// NewDeleteLedgerCommand creates a new DeleteLedgerCommand
func NewDeleteLedgerCommand(id uint32) (*ledgerpb.Command, error) {
	cmdProto := &ledgerpb.DeleteLedgerCommand{
		Id: id,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &ledgerpb.Command{
		Id:   GenerateRandomID(),
		Type: ledgerpb.CommandType_DeleteLedger,
		Data: data,
		Date: ledgerpb.NewTimestamp(time.Now()),
	}, nil
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

// NewCreateLogCommand creates a new command
func NewCreateLogCommand(input *ledgerpb.CommandInput, ledgerID uint32, idempotency *ledgerpb.Idempotency) *ledgerpb.Command {
	cmdProto := &ledgerpb.CreateLogCommand{
		Input:       input,
		Idempotency: idempotency,
		LedgerId:    ledgerID,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		panic(err)
	}

	return &ledgerpb.Command{
		Id:   GenerateRandomID(),
		Type: ledgerpb.CommandType_CreateLog,
		Data: data,
		Date: ledgerpb.NewTimestamp(time.Now()),
	}
}
