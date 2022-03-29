package core

import (
	"crypto/sha256"
	"fmt"
	json "github.com/gibson042/canonicaljson-go"
)

// Transactions struct
type Transactions struct {
	Transactions []TransactionData `json:"transactions" binding:"required,dive"`
}

type TransactionData struct {
	Postings  Postings `json:"postings"`
	Reference string   `json:"reference"`
	Metadata  Metadata `json:"metadata" swaggertype:"object"`
}

type Transaction struct {
	TransactionData
	ID        uint64 `json:"txid"`
	Timestamp string `json:"timestamp"`
}

func (t *Transaction) AppendPosting(p Posting) {
	t.Postings = append(t.Postings, p)
}

func (t *Transaction) Reverse() TransactionData {
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

func Hash(t1, t2 interface{}) string {
	b1, _ := json.Marshal(t1)
	b2, _ := json.Marshal(t2)

	h := sha256.New()
	_, err := h.Write(b1)
	if err != nil {
		panic(err)
	}
	_, err = h.Write(b2)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}
