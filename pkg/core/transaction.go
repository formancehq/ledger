package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions" binding:"required,dive"`
}

type TransactionData struct {
	Postings  Postings  `json:"postings"`
	Reference string    `json:"reference"`
	Metadata  Metadata  `json:"metadata" swaggertype:"object"`
	Timestamp time.Time `json:"timestamp"`
}

func (t TransactionData) SetTimestamp(ts time.Time) TransactionData {
	t.Timestamp = ts
	return t
}

func (t TransactionData) SetMetadata(metadata Metadata) TransactionData {
	t.Metadata = metadata
	return t
}

func (t *TransactionData) Reverse() TransactionData {
	postings := make(Postings, len(t.Postings))
	copy(postings, t.Postings)
	postings.Reverse()

	ret := TransactionData{
		Postings: postings,
	}
	if t.Reference != "" {
		ret.Reference = "revert_" + t.Reference
	}
	return ret
}

func (t TransactionData) SetReference(reference string) TransactionData {
	t.Reference = reference
	return t
}

func NewTransactionData(postings ...Posting) TransactionData {
	return TransactionData{
		Postings: postings,
		Metadata: Metadata{},
	}
}

type Transaction struct {
	TransactionData
	ID uint64 `json:"txid"`
}

type ExpandedTransaction struct {
	Transaction
	PreCommitVolumes  AccountsAssetsVolumes `json:"preCommitVolumes,omitempty"`
	PostCommitVolumes AccountsAssetsVolumes `json:"postCommitVolumes,omitempty"`
}

var _ json.Marshaler = ExpandedTransaction{}

func (t ExpandedTransaction) MarshalJSON() ([]byte, error) {
	type transaction ExpandedTransaction
	return json.Marshal(struct {
		transaction
		Timestamp string `json:"timestamp"`
	}{
		transaction: transaction(t),
		// The std lib format time as RFC3339Nano, use a custom encoding to ensure backward compatibility
		Timestamp: t.Timestamp.Format(time.RFC3339),
	})
}

func (t *ExpandedTransaction) AppendPosting(p Posting) {
	t.Postings = append(t.Postings, p)
}

func (t *ExpandedTransaction) IsReverted() bool {
	if _, ok := t.Metadata[RevertedMetadataSpecKey()]; ok {
		return true
	}
	return false
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
