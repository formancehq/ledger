package system

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	// CommandTypeCreateLedger is the command type for creating a new ledger
	CommandTypeCreateLedger raft.CommandType = "create_ledger"
	// CommandTypeDeleteLedger is the command type for deleting a ledger
	CommandTypeDeleteLedger raft.CommandType = "delete_ledger"
)

// NewCreateLedgerCommand creates a new CreateLedgerCommand
// snapshotThreshold is optional: if nil or 0, uses global config
func NewCreateLedgerCommand(name string, logStoreConfig, runtimeStoreConfig map[string]interface{}, metadata map[string]string, snapshotThreshold *uint64, logStoreDriver, runtimeStoreDriver string) (*raft.Command, error) {
	// Convert log store config map to protobuf Struct
	var logStoreConfigStruct *structpb.Struct
	if logStoreConfig != nil {
		var err error
		logStoreConfigStruct, err = structpb.NewStruct(logStoreConfig)
		if err != nil {
			return nil, err
		}
	}

	// Convert runtime store config map to protobuf Struct
	var runtimeStoreConfigStruct *structpb.Struct
	if runtimeStoreConfig != nil {
		var err error
		runtimeStoreConfigStruct, err = structpb.NewStruct(runtimeStoreConfig)
		if err != nil {
			return nil, err
		}
	}

	cmdProto := &CreateLedgerCommand{
		Name:                name,
		LogStoreDriver:      logStoreDriver,
		RuntimeStoreDriver:  runtimeStoreDriver,
		LogStoreConfig:      logStoreConfigStruct,
		RuntimeStoreConfig:  runtimeStoreConfigStruct,
		Metadata:            metadata,
	}
	if snapshotThreshold != nil && *snapshotThreshold > 0 {
		cmdProto.SnapshotThreshold = *snapshotThreshold
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &raft.Command{
		ID:   raft.GenerateRandomID(),
		Type: CommandTypeCreateLedger,
		Data: data,
		Date: time.Now(),
	}, nil
}

// NewDeleteLedgerCommand creates a new DeleteLedgerCommand
func NewDeleteLedgerCommand(name string) (*raft.Command, error) {
	cmdProto := &DeleteLedgerCommand{
		Name: name,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &raft.Command{
		ID:   raft.GenerateRandomID(),
		Type: CommandTypeDeleteLedger,
		Data: data,
		Date: time.Now(),
	}, nil
}

// UnmarshalCommandData unmarshals FSM command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *CreateLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *DeleteLedgerCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}
