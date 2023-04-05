package core

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"

	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type LogType int16

const (
	SetMetadataLogType         LogType = iota // "SET_METADATA"
	NewTransactionLogType                     // "NEW_TRANSACTION"
	RevertedTransactionLogType                // "REVERTED_TRANSACTION"
)

func (l LogType) String() string {
	switch l {
	case SetMetadataLogType:
		return "SET_METADATA"
	case NewTransactionLogType:
		return "NEW_TRANSACTION"
	case RevertedTransactionLogType:
		return "REVERTED_TRANSACTION"
	}

	return ""
}

// TODO(polo): create Log struct and extended Log struct
type Log struct {
	ID        uint64      `json:"id"`
	Type      LogType     `json:"type"`
	Data      interface{} `json:"data"`
	Hash      string      `json:"hash"`
	Date      Time        `json:"date"`
	Reference string      `json:"reference"`
}

func (l Log) ComputeHash(previous *Log) Log {
	l.Hash = Hash(previous, l)
	return l
}

func (l Log) WithDate(date Time) Log {
	l.Date = date
	return l
}

func (l Log) WithReference(reference string) Log {
	l.Reference = reference
	return l
}

type NewTransactionLogPayload struct {
	Transaction     Transaction                  `json:"transaction"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata"`
}

func NewTransactionLogWithDate(tx Transaction, accountMetadata map[string]metadata.Metadata, time Time) Log {
	// Since the id is unique and the hash is a hash of the previous log, they
	// will be filled at insertion time during the batch process.
	return Log{
		Type: NewTransactionLogType,
		Date: time,
		Data: NewTransactionLogPayload{
			Transaction:     tx,
			AccountMetadata: accountMetadata,
		},
	}
}

func NewTransactionLog(tx Transaction, accountMetadata map[string]metadata.Metadata) Log {
	return NewTransactionLogWithDate(tx, accountMetadata, tx.Timestamp).WithReference(tx.Reference)
}

type SetMetadataLogPayload struct {
	TargetType string            `json:"targetType"`
	TargetID   interface{}       `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

func (s *SetMetadataLogPayload) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string            `json:"targetType"`
		TargetID   json.RawMessage   `json:"targetId"`
		Metadata   metadata.Metadata `json:"metadata"`
	}
	x := X{}
	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}
	var id interface{}
	switch strings.ToUpper(x.TargetType) {
	case strings.ToUpper(MetaTargetTypeAccount):
		id = ""
		err = json.Unmarshal(x.TargetID, &id)
	case strings.ToUpper(MetaTargetTypeTransaction):
		id, err = strconv.ParseUint(string(x.TargetID), 10, 64)
	default:
		panic("unknown type")
	}
	if err != nil {
		return err
	}

	*s = SetMetadataLogPayload{
		TargetType: x.TargetType,
		TargetID:   id,
		Metadata:   x.Metadata,
	}
	return nil
}

func NewSetMetadataLog(at Time, metadata SetMetadataLogPayload) Log {
	// Since the id is unique and the hash is a hash of the previous log, they
	// will be filled at insertion time during the batch process.
	return Log{
		Type: SetMetadataLogType,
		Date: at,
		Data: metadata,
	}
}

type RevertedTransactionLogPayload struct {
	RevertedTransactionID string
	RevertTransaction     Transaction
}

func NewRevertedTransactionLog(at Time, revertedTxID string, tx Transaction) Log {
	return Log{
		Type: RevertedTransactionLogType,
		Date: at,
		Data: RevertedTransactionLogPayload{
			RevertedTransactionID: revertedTxID,
			RevertTransaction:     tx,
		},
	}
}

func HydrateLog(_type LogType, data []byte) (interface{}, error) {
	var payload any
	switch _type {
	case NewTransactionLogType:
		payload = &NewTransactionLogPayload{}
	case SetMetadataLogType:
		payload = &SetMetadataLogPayload{}
	case RevertedTransactionLogType:
		payload = &RevertedTransactionLogPayload{}
	default:
		panic("unknown type " + _type.String())
	}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, err
	}

	return reflect.ValueOf(payload).Elem().Interface(), nil
}

type Accounts map[string]Account

type LogHolder struct {
	Log      *Log
	Ingested chan struct{}
}

func NewLogHolder(log *Log) *LogHolder {
	return &LogHolder{
		Log:      log,
		Ingested: make(chan struct{}),
	}
}
