package system

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	// CommandTypeCreateBucket is the command type for creating a new bucket
	CommandTypeCreateBucket raft.CommandType = "create_bucket"
	// CommandTypeDeleteBucket is the command type for deleting a bucket
	CommandTypeDeleteBucket raft.CommandType = "delete_bucket"
)

// NewCreateBucketCommand creates a new CreateBucketCommand
// snapshotThreshold is optional: if nil or 0, uses global config
func NewCreateBucketCommand(name, driver string, config map[string]interface{}, snapshotThreshold *uint64) (*raft.Command, error) {
	// Convert raftConfig map to protobuf Struct
	configStruct, err := structpb.NewStruct(config)
	if err != nil {
		return nil, err
	}

	cmdProto := &CreateBucketCommand{
		Name:   name,
		Driver: driver,
		Config: configStruct,
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
		Type: CommandTypeCreateBucket,
		Data: data,
		Date: time.Now(),
	}, nil
}

// NewDeleteBucketCommand creates a new DeleteBucketCommand
func NewDeleteBucketCommand(name string) (*raft.Command, error) {
	cmdProto := &DeleteBucketCommand{
		Name: name,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &raft.Command{
		ID:   raft.GenerateRandomID(),
		Type: CommandTypeDeleteBucket,
		Data: data,
		Date: time.Now(),
	}, nil
}

// UnmarshalCommandData unmarshals FSM command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *CreateBucketCommand:
		return proto.Unmarshal(data, cmd)
	case *DeleteBucketCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}
