package fsm

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/commands"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	// CommandTypeCreateBucket is the command type for creating a new bucket
	CommandTypeCreateBucket commands.CommandType = "create_bucket"
	// CommandTypeDeleteBucket is the command type for deleting a bucket
	CommandTypeDeleteBucket commands.CommandType = "delete_bucket"
)

// NewCreateBucketCommand creates a new CreateBucketCommand
func NewCreateBucketCommand(name, driver string, config map[string]interface{}) (*commands.Command, error) {
	// Convert config map to protobuf Struct
	configStruct, err := structpb.NewStruct(config)
	if err != nil {
		return nil, err
	}

	cmdProto := &CreateBucketCommand{
		Name:   name,
		Driver: driver,
		Config: configStruct,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &commands.Command{
		ID:   commands.GenerateRandomID(),
		Type: CommandTypeCreateBucket,
		Data: data,
		Date: time.Now(),
	}, nil
}

// NewDeleteBucketCommand creates a new DeleteBucketCommand
func NewDeleteBucketCommand(name string) (*commands.Command, error) {
	cmdProto := &DeleteBucketCommand{
		Name: name,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &commands.Command{
		ID:   commands.GenerateRandomID(),
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
