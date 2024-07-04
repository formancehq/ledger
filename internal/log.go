package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"github.com/formancehq/stack/libs/go-libs/time"

	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type LogType int16

const (
	// TODO(gfyrag): Create dedicated log type for account and metadata
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

type ChainedLogWithContext struct {
	ChainedLog
	Context context.Context
}

type ChainedLog struct {
	Log
	ID   *big.Int `json:"id"`
	Hash []byte   `json:"hash"`
}

func (l *ChainedLog) WithID(id uint64) *ChainedLog {
	l.ID = big.NewInt(int64(id))
	return l
}

func (l *ChainedLog) UnmarshalJSON(data []byte) error {
	type auxLog ChainedLog
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
	*l = ChainedLog(rawLog.auxLog)
	return err
}

func (l *ChainedLog) ComputeHash(previous *ChainedLog) {
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

type Log struct {
	Type           LogType   `json:"type"`
	Data           any       `json:"data"`
	Date           time.Time `json:"date"`
	IdempotencyKey string    `json:"idempotencyKey"`
}

func (l *Log) WithDate(date time.Time) *Log {
	l.Date = date
	return l
}

func (l *Log) WithIdempotencyKey(key string) *Log {
	l.IdempotencyKey = key
	return l
}

func (l *Log) ChainLog(previous *ChainedLog) *ChainedLog {
	ret := &ChainedLog{
		Log: *l,
		ID:  big.NewInt(0),
	}
	ret.ComputeHash(previous)
	if previous != nil {
		ret.ID = ret.ID.Add(previous.ID, big.NewInt(1))
	}
	return ret
}

type AccountMetadata map[string]metadata.Metadata

type NewTransactionLogPayload struct {
	Transaction     *Transaction    `json:"transaction"`
	AccountMetadata AccountMetadata `json:"accountMetadata"`
}

func NewTransactionLogWithDate(tx *Transaction, accountMetadata map[string]metadata.Metadata, time time.Time) *Log {
	// Since the id is unique and the hash is a hash of the previous log, they
	// will be filled at insertion time during the batch process.
	return &Log{
		Type: NewTransactionLogType,
		Date: time,
		Data: NewTransactionLogPayload{
			Transaction:     tx,
			AccountMetadata: accountMetadata,
		},
	}
}

func NewTransactionLog(tx *Transaction, accountMetadata map[string]metadata.Metadata) *Log {
	return NewTransactionLogWithDate(tx, accountMetadata, time.Now())
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

func NewSetMetadataLog(at time.Time, metadata SetMetadataLogPayload) *Log {
	// Since the id is unique and the hash is a hash of the previous log, they
	// will be filled at insertion time during the batch process.
	return &Log{
		Type: SetMetadataLogType,
		Date: at,
		Data: metadata,
	}
}

type DeleteMetadataLogPayload struct {
	TargetType string `json:"targetType"`
	TargetID   any    `json:"targetId"`
	Key        string `json:"key"`
}

func NewDeleteMetadataLog(at time.Time, payload DeleteMetadataLogPayload) *Log {
	// Since the id is unique and the hash is a hash of the previous log, they
	// will be filled at insertion time during the batch process.
	return &Log{
		Type: DeleteMetadataLogType,
		Date: at,
		Data: payload,
	}
}

func NewSetMetadataOnAccountLog(at time.Time, account string, metadata metadata.Metadata) *Log {
	return &Log{
		Type: SetMetadataLogType,
		Date: at,
		Data: SetMetadataLogPayload{
			TargetType: MetaTargetTypeAccount,
			TargetID:   account,
			Metadata:   metadata,
		},
	}
}

func NewSetMetadataOnTransactionLog(at time.Time, txID *big.Int, metadata metadata.Metadata) *Log {
	return &Log{
		Type: SetMetadataLogType,
		Date: at,
		Data: SetMetadataLogPayload{
			TargetType: MetaTargetTypeTransaction,
			TargetID:   txID,
			Metadata:   metadata,
		},
	}
}

type RevertedTransactionLogPayload struct {
	RevertedTransactionID *big.Int     `json:"revertedTransactionID"`
	RevertTransaction     *Transaction `json:"transaction"`
}

func NewRevertedTransactionLog(at time.Time, revertedTxID *big.Int, tx *Transaction) *Log {
	return &Log{
		Type: RevertedTransactionLogType,
		Date: at,
		Data: RevertedTransactionLogPayload{
			RevertedTransactionID: revertedTxID,
			RevertTransaction:     tx,
		},
	}
}

func HydrateLog(_type LogType, data []byte) (any, error) {
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

func ChainLogs(logs ...*Log) []*ChainedLog {
	var previous *ChainedLog
	ret := make([]*ChainedLog, 0)
	for _, log := range logs {
		next := log.ChainLog(previous)
		ret = append(ret, next)
		previous = next
	}
	return ret
}
