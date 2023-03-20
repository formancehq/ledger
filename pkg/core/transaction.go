package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions" binding:"required,dive"`
}

type TransactionData struct {
	Postings  Postings `json:"postings"`
	Reference string   `json:"reference"`
	Metadata  Metadata `json:"metadata" swaggertype:"object"`
	Timestamp Time     `json:"timestamp"`
}

func (d TransactionData) WithPostings(postings ...Posting) TransactionData {
	d.Postings = append(d.Postings, postings...)
	return d
}

func NewTransactionData() TransactionData {
	return TransactionData{
		Metadata: Metadata{},
	}
}

func (t *TransactionData) Reverse() TransactionData {
	postings := make(Postings, len(t.Postings))
	copy(postings, t.Postings)
	postings.Reverse()

	ret := TransactionData{
		Postings: postings,
	}
	//TODO(gfyra): Do we keep this for v2?
	if t.Reference != "" {
		ret.Reference = "revert_" + t.Reference
	}
	return ret
}

type Transaction struct {
	TransactionData
	ID uint64 `json:"txid"`
}

func (t Transaction) WithPostings(postings ...Posting) Transaction {
	t.TransactionData = t.TransactionData.WithPostings(postings...)
	return t
}

func (t Transaction) WithReference(reference string) Transaction {
	t.Reference = reference
	return t
}

func (t Transaction) WithTimestamp(ts Time) Transaction {
	t.Timestamp = ts
	return t
}

func (t Transaction) WithID(id uint64) Transaction {
	t.ID = id
	return t
}

func (t Transaction) WithMetadata(m Metadata) Transaction {
	t.Metadata = m
	return t
}

func NewTransaction() Transaction {
	return Transaction{
		TransactionData: NewTransactionData(),
	}
}

type ExpandedTransaction struct {
	Transaction
	PreCommitVolumes  AccountsAssetsVolumes `json:"preCommitVolumes,omitempty"`
	PostCommitVolumes AccountsAssetsVolumes `json:"postCommitVolumes,omitempty"`
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

func ExpandTransaction(tx Transaction, preCommitVolumes AccountsAssetsVolumes) ExpandedTransaction {
	postCommitVolumes := preCommitVolumes.copy()
	for _, posting := range tx.Postings {
		preCommitVolumes.AddOutput(posting.Source, posting.Asset, NewMonetaryInt(0))
		preCommitVolumes.AddInput(posting.Source, posting.Asset, NewMonetaryInt(0))
		preCommitVolumes.AddInput(posting.Destination, posting.Asset, NewMonetaryInt(0))
		preCommitVolumes.AddOutput(posting.Destination, posting.Asset, NewMonetaryInt(0))
		postCommitVolumes.AddOutput(posting.Source, posting.Asset, posting.Amount)
		postCommitVolumes.AddInput(posting.Destination, posting.Asset, posting.Amount)
	}
	return ExpandedTransaction{
		Transaction:       tx,
		PreCommitVolumes:  preCommitVolumes,
		PostCommitVolumes: postCommitVolumes,
	}
}

func ExpandTransactionFromEmptyPreCommitVolumes(tx Transaction) ExpandedTransaction {
	return ExpandTransaction(tx, AccountsAssetsVolumes{})
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
