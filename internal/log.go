package ledger

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/metadata"
	"github.com/pkg/errors"
)

type LogType int16

const (
	SetMetadataLogType         LogType = iota // "SET_METADATA"
	NewTransactionLogType                     // "NEW_TRANSACTION"
	RevertedTransactionLogType                // "REVERTED_TRANSACTION"
	DeleteMetadataLogType
)

func (l LogType) String() string {
	switch l {
	case SetMetadataLogType:
		return "SET_METADATA"
	case NewTransactionLogType:
		return "NEW_TRANSACTION"
	case RevertedTransactionLogType:
		return "REVERTED_TRANSACTION"
	case DeleteMetadataLogType:
		return "DELETE_METADATA"
	}

	return ""
}

func LogTypeFromString(logType string) LogType {
	switch logType {
	case "SET_METADATA":
		return SetMetadataLogType
	case "NEW_TRANSACTION":
		return NewTransactionLogType
	case "REVERTED_TRANSACTION":
		return RevertedTransactionLogType
	case "DELETE_METADATA":
		return DeleteMetadataLogType
	}

	panic(errors.New("invalid log type"))
}

// Needed in order to keep the compatibility with the openapi response for
// ListLogs.
func (lt LogType) MarshalJSON() ([]byte, error) {
	return json.Marshal(lt.String())
}

func (lt *LogType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	*lt = LogTypeFromString(s)

	return nil
}

// notes(gfyrag): keep key ordered! the order matter when hashing the log.
type Log struct {
	Type           LogType   `json:"type"`
	Data           any       `json:"data"`
	Date           time.Time `json:"date"`
	IdempotencyKey string    `json:"idempotencyKey"`
	ID             int       `json:"id"`
	Hash           []byte    `json:"hash"`
}

func (l Log) WithIdempotencyKey(key string) Log {
	l.IdempotencyKey = key
	return l
}

func (l Log) ChainLog(previous *Log) Log {
	ret := l
	ret.ComputeHash(previous)
	if previous != nil {
		ret.ID = previous.ID + 1
	} else {
		ret.ID = 1
	}
	return ret
}

func (l *Log) UnmarshalJSON(data []byte) error {
	type auxLog Log
	type log struct {
		auxLog
		Data json.RawMessage `json:"data"`
	}
	rawLog := log{}
	if err := json.Unmarshal(data, &rawLog); err != nil {
		return err
	}

	var err error
	rawLog.auxLog.Data, err = HydrateLog(rawLog.Type, rawLog.Data)
	if err != nil {
		return err
	}
	*l = Log(rawLog.auxLog)
	return err
}

func (l *Log) ComputeHash(previous *Log) {
	digest := sha256.New()
	enc := json.NewEncoder(digest)

	if previous != nil {
		if err := enc.Encode(previous.Hash); err != nil {
			panic(err)
		}
	}

	if err := enc.Encode(l); err != nil {
		panic(err)
	}

	l.Hash = digest.Sum(nil)
}

func NewLog(t LogType, payload any) Log {
	return Log{
		Type: t,
		Data: payload,
		Date: time.Now(),
	}
}

type AccountMetadata map[string]metadata.Metadata

type NewTransactionLogPayload struct {
	Transaction     Transaction     `json:"transaction"`
	AccountMetadata AccountMetadata `json:"accountMetadata"`
}

func NewTransactionLog(tx Transaction, accountMetadata AccountMetadata) Log {
	if accountMetadata == nil {
		accountMetadata = AccountMetadata{}
	}
	return NewLog(NewTransactionLogType, NewTransactionLogPayload{
		Transaction:     tx,
		AccountMetadata: accountMetadata,
	})
}

type SetMetadataLogPayload struct {
	TargetType string            `json:"targetType"`
	TargetID   any               `json:"targetId"`
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
		id, err = strconv.ParseInt(string(x.TargetID), 10, 64)
		id = int(id.(int64))
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

func NewSetMetadataOnAccountLog(account string, metadata metadata.Metadata) Log {
	return NewLog(SetMetadataLogType, SetMetadataLogPayload{
		TargetType: MetaTargetTypeAccount,
		TargetID:   account,
		Metadata:   metadata,
	})
}

func NewSetMetadataOnTransactionLog(txID int, metadata metadata.Metadata) Log {
	return NewLog(SetMetadataLogType, SetMetadataLogPayload{
		TargetType: MetaTargetTypeTransaction,
		TargetID:   txID,
		Metadata:   metadata,
	})
}

type DeleteMetadataLogPayload struct {
	TargetType string `json:"targetType"`
	TargetID   any    `json:"targetId"`
	Key        string `json:"key"`
}

func (s *DeleteMetadataLogPayload) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string          `json:"targetType"`
		TargetID   json.RawMessage `json:"targetId"`
		Key        string          `json:"key"`
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
		id, err = strconv.ParseInt(string(x.TargetID), 10, 64)
		id = int(id.(int64))
	default:
		return fmt.Errorf("unknown type " + x.TargetType)
	}
	if err != nil {
		return err
	}

	*s = DeleteMetadataLogPayload{
		TargetType: x.TargetType,
		TargetID:   id,
		Key:        x.Key,
	}
	return nil
}

func NewDeleteTransactionMetadataLog(id int, key string) Log {
	return NewLog(DeleteMetadataLogType, DeleteMetadataLogPayload{
		TargetType: MetaTargetTypeTransaction,
		TargetID:   id,
		Key:        key,
	})
}

func NewDeleteAccountMetadataLog(id string, key string) Log {
	return NewLog(DeleteMetadataLogType, DeleteMetadataLogPayload{
		TargetType: MetaTargetTypeAccount,
		TargetID:   id,
		Key:        key,
	})
}

type RevertedTransactionLogPayload struct {
	RevertedTransactionID int         `json:"revertedTransactionID"`
	RevertTransaction     Transaction `json:"transaction"`
}

func NewRevertedTransactionLog(revertedTxID int, tx Transaction) Log {
	return NewLog(RevertedTransactionLogType, RevertedTransactionLogPayload{
		RevertedTransactionID: revertedTxID,
		RevertTransaction:     tx,
	})
}

func HydrateLog(_type LogType, data []byte) (any, error) {
	var payload any
	switch _type {
	case NewTransactionLogType:
		payload = &NewTransactionLogPayload{}
	case SetMetadataLogType:
		payload = &SetMetadataLogPayload{}
	case DeleteMetadataLogType:
		payload = &DeleteMetadataLogPayload{}
	case RevertedTransactionLogType:
		payload = &RevertedTransactionLogPayload{}
	default:
		return nil, fmt.Errorf("unknown type " + _type.String())
	}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, err
	}

	return reflect.ValueOf(payload).Elem().Interface(), nil
}
