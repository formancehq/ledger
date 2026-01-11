package system

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/systempb"
	"google.golang.org/protobuf/proto"
)

// NewCreateLedgerCommand creates a new CreateLedgerCommand
// snapshotThreshold is optional: if nil or 0, uses global config
func NewCreateLedgerCommand(cmd *systempb.CreateLedgerRequest) *raft.Command {

	data, err := proto.Marshal(cmd)
	if err != nil {
		panic(err)
	}

	return &raft.Command{
		Id:   raft.GenerateRandomID(),
		Type: raft.CommandType_CreateLedger,
		Data: data,
		Date: ledgerpb.NewTimestamp(time.Now()),
	}
}

// NewDeleteLedgerCommand creates a new DeleteLedgerCommand
func NewDeleteLedgerCommand(name string) (*raft.Command, error) {
	cmdProto := &systempb.DeleteLedgerRequest{
		Name: name,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &raft.Command{
		Id:   raft.GenerateRandomID(),
		Type: raft.CommandType_DeleteLedger,
		Data: data,
		Date: ledgerpb.NewTimestamp(time.Now()),
	}, nil
}

// UnmarshalCommandData unmarshals FSM command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *systempb.CreateLedgerRequest:
		return proto.Unmarshal(data, cmd)
	case *systempb.DeleteLedgerRequest:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}
