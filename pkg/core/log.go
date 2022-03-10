package core

import (
	"encoding/json"
	"time"
)

const SetMetadataType = "SET_METADATA"
const NewTransactionType = "NEW_TRANSACTION"

type Log struct {
	ID   uint64      `json:"id"`
	Type string      `json:"hash"`
	Data interface{} `json:"data"`
	Hash string      `json:"hash"`
	Date time.Time   `json:"date"`
}

func NewTransactionLog(previousLog *Log, tx Transaction) Log {
	id := uint64(0)
	if previousLog != nil {
		id = previousLog.ID + 1
	}
	l := Log{
		ID:   id,
		Type: NewTransactionType,
		Date: time.Now(),
		Data: tx,
	}
	l.Hash = Hash(previousLog, &l)
	return l
}

type SetMetadata struct {
	TargetType string   `json:"targetType"`
	TargetID   string   `json:"targetId"`
	Metadata   Metadata `json:"metadata"`
}

func NewSetMetadataLog(previousLog *Log, metadata SetMetadata) Log {
	id := uint64(0)
	if previousLog != nil {
		id = previousLog.ID + 1
	}
	l := Log{
		ID:   id,
		Type: SetMetadataType,
		Date: time.Now(),
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
