package ledger

import (
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/uptrace/bun"
	"reflect"
	"strconv"
	"strings"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/metadata"
)

const (
	SetMetadataLogType         LogType = iota // "SET_METADATA"
	NewLogType                                // "NEW_TRANSACTION"
	RevertedTransactionLogType                // "REVERTED_TRANSACTION"
	DeleteMetadataLogType
)

type LogType int16

func (lt LogType) Value() (driver.Value, error) {
	return lt.String(), nil
}

func (lt *LogType) Scan(src interface{}) error {
	*lt = LogTypeFromString(string(src.([]byte)))
	return nil
}

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

func (lt LogType) String() string {
	switch lt {
	case SetMetadataLogType:
		return "SET_METADATA"
	case NewLogType:
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
		return NewLogType
	case "REVERTED_TRANSACTION":
		return RevertedTransactionLogType
	case "DELETE_METADATA":
		return DeleteMetadataLogType
	}

	panic("invalid log type")
}

// Log represents atomic actions made on the ledger.
type Log struct {
	bun.BaseModel `bun:"table:logs,alias:logs"`

	Type           LogType    `json:"type" bun:"type,type:log_type"`
	Data           LogPayload `json:"data" bun:"data,type:jsonb"`
	Date           time.Time  `json:"date" bun:"date,type:timestamptz,nullzero"`
	IdempotencyKey string     `json:"idempotencyKey" bun:"idempotency_key,type:varchar(256),unique,nullzero"`
	// IdempotencyHash is a signature used when using IdempotencyKey.
	// It allows to check if the usage of IdempotencyKey match inputs given on the first idempotency key usage.
	IdempotencyHash string `json:"idempotencyHash" bun:"idempotency_hash,unique,nullzero"`
	ID              int    `json:"id" bun:"id,unique,type:numeric"`
	Hash            []byte `json:"hash" bun:"hash,type:bytea,scanonly"`
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

	payload := l.Data.(any)
	if hv, ok := payload.(Memento); ok {
		payload = hv.GetMemento()
	}

	if err := enc.Encode(struct {
		// notes(gfyrag): Keep keys ordered! the order matter when hashing the log.
		Type           LogType   `json:"type"`
		Data           any       `json:"data"`
		Date           time.Time `json:"date"`
		IdempotencyKey string    `json:"idempotencyKey"`
		ID             int       `json:"id"`
		Hash           []byte    `json:"hash"`
	}{
		Type:           l.Type,
		Data:           payload,
		Date:           l.Date,
		IdempotencyKey: l.IdempotencyKey,
		ID:             l.ID,
		Hash:           l.Hash,
	}); err != nil {
		panic(err)
	}

	l.Hash = digest.Sum(nil)
}

func NewLog(payload LogPayload) Log {
	return Log{
		Type: payload.Type(),
		Data: payload,
	}
}

type LogPayload interface {
	Type() LogType
}

type Memento interface {
	GetMemento() any
}

type AccountMetadata map[string]metadata.Metadata

type CreatedTransaction struct {
	Transaction     Transaction     `json:"transaction"`
	AccountMetadata AccountMetadata `json:"accountMetadata"`
}

func (p CreatedTransaction) Type() LogType {
	return NewLogType
}

var _ LogPayload = (*CreatedTransaction)(nil)

func (p CreatedTransaction) GetMemento() any {
	// Exclude postCommitVolumes and postCommitEffectiveVolumes fields from transactions.
	// We don't want those fields to be part of the hash as they are not part of the decision-making process.
	type transactionResume struct {
		Postings  Postings          `json:"postings"`
		Metadata  metadata.Metadata `json:"metadata"`
		Timestamp time.Time         `json:"timestamp"`
		Reference string            `json:"reference,omitempty"`
		ID        int               `json:"id"`
		Reverted  bool              `json:"reverted"`
	}

	return struct {
		Transaction     transactionResume `json:"transaction"`
		AccountMetadata AccountMetadata   `json:"accountMetadata"`
	}{
		Transaction: transactionResume{
			Postings:  p.Transaction.Postings,
			Metadata:  p.Transaction.Metadata,
			Timestamp: p.Transaction.Timestamp,
			Reference: p.Transaction.Reference,
			ID:        p.Transaction.ID,
		},
		AccountMetadata: p.AccountMetadata,
	}
}

var _ Memento = (*CreatedTransaction)(nil)

type SavedMetadata struct {
	TargetType string            `json:"targetType"`
	TargetID   any               `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

func (s SavedMetadata) Type() LogType {
	return SetMetadataLogType
}

var _ LogPayload = (*SavedMetadata)(nil)

func (s *SavedMetadata) UnmarshalJSON(data []byte) error {
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

	*s = SavedMetadata{
		TargetType: x.TargetType,
		TargetID:   id,
		Metadata:   x.Metadata,
	}
	return nil
}

type DeletedMetadata struct {
	TargetType string `json:"targetType"`
	TargetID   any    `json:"targetId"`
	Key        string `json:"key"`
}

func (s DeletedMetadata) Type() LogType {
	return DeleteMetadataLogType
}

var _ LogPayload = (*DeletedMetadata)(nil)

func (s *DeletedMetadata) UnmarshalJSON(data []byte) error {
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
		return fmt.Errorf("unknown type '%s'", x.TargetType)
	}
	if err != nil {
		return err
	}

	*s = DeletedMetadata{
		TargetType: x.TargetType,
		TargetID:   id,
		Key:        x.Key,
	}
	return nil
}

type RevertedTransaction struct {
	RevertedTransaction Transaction `json:"revertedTransaction"`
	RevertTransaction   Transaction `json:"transaction"`
}

func (r RevertedTransaction) Type() LogType {
	return RevertedTransactionLogType
}

var _ LogPayload = (*RevertedTransaction)(nil)

func (r RevertedTransaction) GetMemento() any {

	type transactionResume struct {
		Postings  Postings          `json:"postings"`
		Metadata  metadata.Metadata `json:"metadata"`
		Timestamp time.Time         `json:"timestamp"`
		Reference string            `json:"reference,omitempty"`
		ID        int               `json:"id"`
		Reverted  bool              `json:"reverted"`
	}

	return struct {
		RevertedTransactionID int               `json:"revertedTransactionID"`
		RevertTransaction     transactionResume `json:"transaction"`
	}{
		RevertedTransactionID: r.RevertedTransaction.ID,
		RevertTransaction: transactionResume{
			Postings:  r.RevertTransaction.Postings,
			Metadata:  r.RevertTransaction.Metadata,
			Timestamp: r.RevertTransaction.Timestamp,
			Reference: r.RevertTransaction.Reference,
			ID:        r.RevertTransaction.ID,
		},
	}
}

var _ Memento = (*RevertedTransaction)(nil)

func HydrateLog(_type LogType, data []byte) (LogPayload, error) {
	var payload any
	switch _type {
	case NewLogType:
		payload = &CreatedTransaction{}
	case SetMetadataLogType:
		payload = &SavedMetadata{}
	case DeleteMetadataLogType:
		payload = &DeletedMetadata{}
	case RevertedTransactionLogType:
		payload = &RevertedTransaction{}
	default:
		return nil, fmt.Errorf("unknown type '%s'", _type)
	}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, err
	}

	return reflect.ValueOf(payload).Elem().Interface().(LogPayload), nil
}

func ComputeIdempotencyHash(inputs any) string {
	digest := sha256.New()
	enc := json.NewEncoder(digest)

	if err := enc.Encode(inputs); err != nil {
		panic(err)
	}

	return base64.URLEncoding.EncodeToString(digest.Sum(nil))
}
