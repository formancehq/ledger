package ledger

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"google.golang.org/protobuf/proto"
)

// NewCreateLogCommand creates a new command
func NewCreateLogCommand(input *ledgerpb.CommandInput, idempotency *ledgerpb.Idempotency) (*raft.Command, error) {
	cmdProto := &CreateLogCommand{
		Input: input,
		Idempotency: idempotency,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &raft.Command{
		Id:   raft.GenerateRandomID(),
		Type: raft.CommandType_InsertLog,
		Data: data,
		Date: ledgerpb.NewTimestamp(time.Now()),
	}, nil
}

// UnmarshalCommandData unmarshals ledger command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *CreateLogCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}
