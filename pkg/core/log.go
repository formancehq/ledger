package core

import (
	"strconv"
	"strings"
	"time"

	json "github.com/gibson042/canonicaljson-go"
)

const SetMetadataType = "SET_METADATA"
const NewTransactionType = "NEW_TRANSACTION"

type LoggedTX Transaction

func (m LoggedTX) MarshalJSON() ([]byte, error) {
	metadata := make(map[string]interface{})
	for k, v := range m.Metadata {
		var i interface{}
		err := json.Unmarshal(v, &i)
		if err != nil {
			return nil, err
		}
		metadata[k] = i
	}
	type transaction Transaction
	return json.Marshal(struct {
		transaction
		Metadata  map[string]interface{} `json:"metadata"`
		Timestamp string                 `json:"timestamp"`
	}{
		transaction: transaction(m),
		Metadata:    metadata,
		Timestamp:   m.Timestamp.Format(time.RFC3339),
	})
}

type Log struct {
	ID   uint64      `json:"id"`
	Type string      `json:"type"`
	Data interface{} `json:"data"`
	Hash string      `json:"hash"`
	Date time.Time   `json:"date"`
}

func NewTransactionLogWithDate(previousLog *Log, tx Transaction, time time.Time) Log {
	id := uint64(0)
	if previousLog != nil {
		id = previousLog.ID + 1
	}
	l := Log{
		ID:   id,
		Type: NewTransactionType,
		Date: time,
		Data: LoggedTX(tx),
	}
	l.Hash = Hash(previousLog, &l)
	return l
}

func NewTransactionLog(previousLog *Log, tx Transaction) Log {
	return NewTransactionLogWithDate(previousLog, tx, tx.Timestamp)
}

type SetMetadata struct {
	TargetType string      `json:"targetType"`
	TargetID   interface{} `json:"targetId"`
	Metadata   Metadata    `json:"metadata"`
}

func (s *SetMetadata) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string          `json:"targetType"`
		TargetID   json.RawMessage `json:"targetId"`
		Metadata   Metadata        `json:"metadata"`
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

	*s = SetMetadata{
		TargetType: x.TargetType,
		TargetID:   id,
		Metadata:   x.Metadata,
	}
	return nil
}

func NewSetMetadataLog(previousLog *Log, metadata SetMetadata) Log {
	id := uint64(0)
	if previousLog != nil {
		id = previousLog.ID + 1
	}
	l := Log{
		ID:   id,
		Type: SetMetadataType,
		Date: time.Now().UTC().Truncate(time.Second),
		Data: metadata,
	}
	l.Hash = Hash(previousLog, &l)
	return l
}

func HydrateLog(_type string, data string) (interface{}, error) {
	switch _type {
	case NewTransactionType:
		tx := Transaction{}
		err := json.Unmarshal([]byte(data), &tx)
		if err != nil {
			return nil, err
		}
		return tx, nil
	case SetMetadataType:
		sm := SetMetadata{}
		err := json.Unmarshal([]byte(data), &sm)
		if err != nil {
			return nil, err
		}
		return sm, nil
	default:
		panic("unknown type " + _type)
	}
}
