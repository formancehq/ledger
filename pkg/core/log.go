package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/formancehq/stack/libs/go-libs/collectionutils"
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

type hashable interface {
	hashString(buf *buffer)
}

// TODO(polo): create Log struct and extended Log struct
type Log struct {
	ID        uint64   `json:"id"`
	Type      LogType  `json:"type"`
	Data      hashable `json:"data"`
	Hash      []byte   `json:"hash"`
	Date      Time     `json:"date"`
	Reference string   `json:"reference"`
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

	buf := bufferPool.Get().(*buffer)
	defer func() {
		buf.reset()
		bufferPool.Put(buf)
	}()
	hashLog := func(l *Log) {
		buf.writeUInt64(l.ID)
		buf.writeUInt16(uint16(l.Type))
		buf.writeUInt64(uint64(l.Date.UnixNano()))
		buf.writeString(l.Reference)
		l.Data.hashString(buf)
	}

	if previous != nil {
		hashLog(previous)
	}
	hashLog(l)

	h := sha256.New()
	_, err := h.Write(buf.bytes())
	if err != nil {
		panic(err)
	}

	l.Hash = h.Sum(nil)
}

func (l Log) WithDate(date Time) Log {
	l.Date = date
	return l
}

func (l Log) WithReference(reference string) Log {
	l.Reference = reference
	return l
}

type AccountMetadata map[string]metadata.Metadata

func (m AccountMetadata) hashString(buf *buffer) {
	if len(m) == 0 {
		return
	}
	accounts := collectionutils.Keys(m)
	if len(accounts) > 1 {
		sort.Strings(accounts)
	}

	for _, account := range accounts {
		buf.writeString(account)
		hashStringMetadata(buf, m[account])
	}
}

type NewTransactionLogPayload struct {
	Transaction     Transaction     `json:"transaction"`
	AccountMetadata AccountMetadata `json:"accountMetadata"`
}

func (n NewTransactionLogPayload) hashString(buf *buffer) {
	n.AccountMetadata.hashString(buf)
	n.Transaction.hashString(buf)
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
	TargetID   any               `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

func (s SetMetadataLogPayload) hashString(buf *buffer) {
	buf.writeString(s.TargetType)
	switch targetID := s.TargetID.(type) {
	case string:
		buf.writeString(targetID)
	case uint64:
		buf.writeUInt64(targetID)
	}
	hashStringMetadata(buf, s.Metadata)
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
	RevertedTransactionID uint64
	RevertTransaction     Transaction
}

func (r RevertedTransactionLogPayload) hashString(buf *buffer) {
	buf.writeUInt64(r.RevertedTransactionID)
	r.RevertTransaction.hashString(buf)
}

func NewRevertedTransactionLog(at Time, revertedTxID uint64, tx Transaction) Log {
	return Log{
		Type: RevertedTransactionLogType,
		Date: at,
		Data: RevertedTransactionLogPayload{
			RevertedTransactionID: revertedTxID,
			RevertTransaction:     tx,
		},
	}
}

func HydrateLog(_type LogType, data []byte) (hashable, error) {
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

	return reflect.ValueOf(payload).Elem().Interface().(hashable), nil
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

type buffer struct {
	buf *bytes.Buffer
}

func (b *buffer) must(err error) {
	if err != nil {
		panic(err)
	}
}

func (b *buffer) mustWithValue(v any, err error) {
	if err != nil {
		panic(err)
	}
}

func (b *buffer) writeUInt64(v uint64) {
	b.must(b.buf.WriteByte(byte((v >> 56) & 0xFF)))
	b.must(b.buf.WriteByte(byte((v >> 48) & 0xFF)))
	b.must(b.buf.WriteByte(byte((v >> 40) & 0xFF)))
	b.must(b.buf.WriteByte(byte((v >> 32) & 0xFF)))
	b.must(b.buf.WriteByte(byte(v >> 24)))
	b.must(b.buf.WriteByte(byte((v >> 16) & 0xFF)))
	b.must(b.buf.WriteByte(byte((v >> 8) & 0xFF)))
	b.must(b.buf.WriteByte(byte(v & 0xFF)))
}

func (b *buffer) writeUInt16(v uint16) {
	b.must(b.buf.WriteByte(byte((v >> 8) & 0xFF)))
	b.must(b.buf.WriteByte(byte(v & 0xFF)))
}

func (b *buffer) writeString(v string) {
	b.mustWithValue(b.buf.WriteString(v))
}

func (b *buffer) reset() {
	b.buf.Reset()
}

func (b *buffer) bytes() []byte {
	return b.buf.Bytes()
}

func (b *buffer) write(bytes []byte) {
	b.mustWithValue(b.buf.Write(bytes))
}

var (
	bufferPool = sync.Pool{
		New: func() any {
			return &buffer{
				buf: bytes.NewBuffer(make([]byte, 4096)),
			}
		},
	}
)
