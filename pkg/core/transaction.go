package core

import (
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions"`
}

type TransactionData struct {
	Postings  Postings          `json:"postings"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata"`
	Timestamp Time              `json:"timestamp"`
}

func (d TransactionData) WithPostings(postings ...Posting) TransactionData {
	d.Postings = append(d.Postings, postings...)
	return d
}

func NewTransactionData() TransactionData {
	return TransactionData{
		Metadata: metadata.Metadata{},
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

func (d TransactionData) hashString(buf *buffer) {
	buf.writeString(d.Reference)
	buf.writeUInt64(uint64(d.Timestamp.UnixNano()))
	hashStringMetadata(buf, d.Metadata)
	for _, posting := range d.Postings {
		posting.hashString(buf)
	}
}

type Transaction struct {
	TransactionData
	ID uint64 `json:"txid"`
}

type TransactionWithMetadata struct {
	ID       uint64
	Metadata metadata.Metadata
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

func (t Transaction) WithMetadata(m metadata.Metadata) Transaction {
	t.Metadata = m
	return t
}

func (t Transaction) hashString(buf *buffer) {
	buf.writeUInt64(t.ID)
	t.TransactionData.hashString(buf)
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
	return IsReverted(t.Metadata)
}

func ExpandTransaction(tx Transaction, preCommitVolumes AccountsAssetsVolumes) ExpandedTransaction {
	postCommitVolumes := preCommitVolumes.copy()
	for _, posting := range tx.Postings {
		preCommitVolumes.AddInput(posting.Destination, posting.Asset, Zero)
		preCommitVolumes.AddOutput(posting.Source, posting.Asset, Zero)
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
