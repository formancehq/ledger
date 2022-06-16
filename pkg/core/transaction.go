package core

import (
	"crypto/sha256"
	"fmt"

	json "github.com/gibson042/canonicaljson-go"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions" binding:"required,dive"`
}

type TransactionData struct {
	Postings  Postings `json:"postings"`
	Reference string   `json:"reference"`
	Metadata  Metadata `json:"metadata" swaggertype:"object"`
}

func (t *TransactionData) Reverse() TransactionData {
	postings := t.Postings
	postings.Reverse()

	ret := TransactionData{
		Postings: postings,
	}
	if t.Reference != "" {
		ret.Reference = "revert_" + t.Reference
	}
	return ret
}

type Transaction struct {
	TransactionData
	ID                uint64          `json:"txid"`
	Timestamp         string          `json:"timestamp"`
	PreCommitVolumes  AccountsVolumes `json:"preCommitVolumes,omitempty"`  // Keep omitempty to keep consistent hash
	PostCommitVolumes AccountsVolumes `json:"postCommitVolumes,omitempty"` // Keep omitempty to keep consistent hash
}

func (t *Transaction) AppendPosting(p Posting) {
	t.Postings = append(t.Postings, p)
}

func Hash(t1, t2 interface{}) string {
	b1, err := json.Marshal(t1)
	if err != nil {
		panic(err)
	}
	b2, err := json.Marshal(t2)
	if err != nil {
		panic(err)
	}

	h := sha256.New()
	_, err = h.Write(b1)
	if err != nil {
		panic(err)
	}
	_, err = h.Write(b2)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func CheckHash(logs ...Log) (int, bool) {
	for i := len(logs) - 1; i >= 0; i-- {
		var lastLog *Log
		if i < len(logs)-1 {
			lastLog = &logs[i+1]
		}
		log := logs[i]
		log.Hash = ""
		h := Hash(lastLog, log)
		if logs[i].Hash != h {
			return i, false
		}
	}
	return 0, true
}
