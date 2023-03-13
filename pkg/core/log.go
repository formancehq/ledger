package core

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const SetMetadataType = "SET_METADATA"
const NewTransactionType = "NEW_TRANSACTION"

// TODO(polo): create Log struct and extended Log struct
type Log struct {
	ID   uint64      `json:"id"`
	Type string      `json:"type"`
	Data interface{} `json:"data"`
	Hash string      `json:"hash"`
	Date time.Time   `json:"date"`
}

func NewTransactionLogWithDate(tx Transaction, time time.Time) Log {
	// Since the id is unique and the hash is a hash of the previous log, they
	// will be filled at insertion time during the batch process.
	return Log{
		Type: NewTransactionType,
		Date: time,
		Data: tx,
	}
}

func NewTransactionLog(tx Transaction) Log {
	return NewTransactionLogWithDate(tx, tx.Timestamp)
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

func NewSetMetadataLog(at time.Time, metadata SetMetadata) Log {
	// Since the id is unique and the hash is a hash of the previous log, they
	// will be filled at insertion time during the batch process.
	return Log{
		Type: SetMetadataType,
		Date: at,
		Data: metadata,
	}
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

type Accounts map[string]Account

func (a Accounts) ensureExists(accounts ...string) {
	for _, account := range accounts {
		_, ok := a[account]
		if !ok {
			a[account] = Account{
				Address:  AccountAddress(account),
				Metadata: Metadata{},
			}
		}
	}
}

type LogProcessor struct {
	Transactions []*ExpandedTransaction
	Accounts     Accounts
	Volumes      AccountsAssetsVolumes
}

func (m *LogProcessor) ProcessNextLog(logs ...Log) {
	for _, log := range logs {
		switch log.Type {
		case NewTransactionType:
			tx := ExpandedTransaction{
				Transaction:       log.Data.(Transaction),
				PreCommitVolumes:  AccountsAssetsVolumes{},
				PostCommitVolumes: AccountsAssetsVolumes{},
			}
			m.Transactions = append(m.Transactions, &tx)
			for _, posting := range tx.Postings {
				tx.PreCommitVolumes.SetVolumes(posting.Source, posting.Asset, m.Volumes.GetVolumes(posting.Source, posting.Asset))
				tx.PreCommitVolumes.SetVolumes(posting.Destination, posting.Asset, m.Volumes.GetVolumes(posting.Destination, posting.Asset))
			}
			for _, posting := range tx.Postings {
				m.Accounts.ensureExists(posting.Source, posting.Destination)
				m.Volumes.AddOutput(posting.Source, posting.Asset, posting.Amount)
				m.Volumes.AddInput(posting.Destination, posting.Asset, posting.Amount)
			}
			for _, posting := range tx.Postings {
				tx.PostCommitVolumes.SetVolumes(posting.Source, posting.Asset, m.Volumes.GetVolumes(posting.Source, posting.Asset))
				tx.PostCommitVolumes.SetVolumes(posting.Destination, posting.Asset, m.Volumes.GetVolumes(posting.Destination, posting.Asset))
			}
		case SetMetadataType:
			setMetadata := log.Data.(SetMetadata)
			switch setMetadata.TargetType {
			case MetaTargetTypeAccount:
				account := setMetadata.TargetID.(string)
				m.Accounts.ensureExists(account)
				m.Accounts[account].Metadata.Merge(setMetadata.Metadata)
			case MetaTargetTypeTransaction:
				id := setMetadata.TargetID.(int)
				m.Transactions[id].Metadata.Merge(setMetadata.Metadata)
			}
		}
	}
}

func NewLogProcessor() *LogProcessor {
	return &LogProcessor{
		Transactions: make([]*ExpandedTransaction, 0),
		Accounts:     Accounts{},
		Volumes:      AccountsAssetsVolumes{},
	}
}
