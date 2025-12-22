package ledger

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"google.golang.org/protobuf/proto"
)

const (
	// CommandTypeInsertLog is the command type for inserting a log
	CommandTypeInsertLog raft.CommandType = "insert_log"
)

// NewInsertLogCommand creates a new InsertLogCommand
func NewInsertLogCommand(log *ledgerpb.Log) (*raft.Command, error) {
	cmdProto := &InsertLogCommand{
		Log: log, // log is *ledgerpb.Log, which matches InsertLogCommand.Log type
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &raft.Command{
		ID:   raft.GenerateRandomID(),
		Type: CommandTypeInsertLog,
		Data: data,
		Date: time.Now(),
	}, nil
}

// UnmarshalCommandData unmarshals ledger command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *InsertLogCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}
