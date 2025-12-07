package bucketfsm

import (
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	// CommandTypeCreateLedger is the command type for creating a new ledger
	CommandTypeCreateLedger service.CommandType = "create_ledger"
	// CommandTypeInsertLog is the command type for inserting a log
	CommandTypeInsertLog service.CommandType = "insert_log"
)

// NewCreateLedgerCommand creates a new CreateLedgerCommand
func NewCreateLedgerCommand(name string, md metadata.Metadata) (*service.Command, error) {
	var mdStruct *structpb.Struct
	var err error
	if len(md) > 0 {
		mdStruct, err = metadataToStruct(md)
		if err != nil {
			return nil, err
		}
	}

	cmdProto := &CreateLedgerCommand{
		Name:     name,
		Metadata: mdStruct,
	}

	data, err := proto.Marshal(cmdProto)
	if err != nil {
		return nil, err
	}

	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeCreateLedger,
		Data: data,
		Date: time.Now(),
	}, nil
}

// NewInsertLogCommand creates a new InsertLogCommand
func NewInsertLogCommand(log ledger.Log) (*service.Command, error) {
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

	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeInsertLog,
		Data: data,
		Date: time.Now(),
	}, nil
}

// UnmarshalCommandData unmarshals command data from binary format using protobuf
func UnmarshalCommandData(data []byte, v interface{}) error {
	switch cmd := v.(type) {
	case *CreateLedgerCommand:
		return proto.Unmarshal(data, cmd)
	case *InsertLogCommand:
		return proto.Unmarshal(data, cmd)
	default:
		return proto.Unmarshal(data, v.(proto.Message))
	}
}
