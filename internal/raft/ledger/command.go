package ledger

import (
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"google.golang.org/protobuf/proto"
)

const (
	// CommandTypeInsertLog is the command type for inserting a log
	CommandTypeInsertLog raft.CommandType = "insert_log"
)

// NewInsertLogCommand creates a new InsertLogCommand
func NewInsertLogCommand(log ledger.Log) (*raft.Command, error) {
	logProto, err := logToProto(log)
	if err != nil {
		return nil, err
	}

	cmdProto := &InsertLogCommand{
		Log: logProto,
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

