package commonpb

import (
	"slices"
	"sort"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
)

// NewTransaction creates a new Transaction with empty metadata.
func NewTransaction() *Transaction {
	return &Transaction{
		Metadata: &MetadataSet{},
	}
}

// WithPostings adds postings to Transaction.
func (tx *Transaction) WithPostings(postings ...*Posting) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.Postings = append(tx.Postings, postings...)

	return tx
}

// WithID sets the ID of the transaction.
func (tx *Transaction) WithID(id uint64) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.Id = id

	return tx
}

// WithReference sets the reference of the transaction.
func (tx *Transaction) WithReference(ref string) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.Reference = ref

	return tx
}

// WithTimestamp sets the timestamp of the transaction.
func (tx *Transaction) WithTimestamp(ts time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.Timestamp = NewTimestamp(ts)

	return tx
}

// WithMetadata sets the metadata of the transaction.
func (tx *Transaction) WithMetadata(m metadata.Metadata) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.Metadata = MetadataSetFromMap(m)

	return tx
}

// WithInsertedAt sets the inserted_at timestamp.
func (tx *Transaction) WithInsertedAt(date time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.InsertedAt = NewTimestamp(date)

	return tx
}

// WithUpdatedAt sets the updated_at timestamp.
func (tx *Transaction) WithUpdatedAt(at time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.UpdatedAt = NewTimestamp(at)

	return tx
}

// WithRevertedAt sets the reverted_at timestamp and marks the transaction as reverted.
func (tx *Transaction) WithRevertedAt(timestamp time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.RevertedAt = NewTimestamp(timestamp)
	tx.Reverted = true

	return tx
}

// IsReverted returns true if the transaction is reverted.
func (tx *Transaction) IsReverted() bool {
	if tx == nil {
		return false
	}

	return tx.GetReverted() || tx.GetRevertedAt() != nil
}

// Reverse creates a reversed copy of the transaction with swapped source/destination in postings.
func (tx *Transaction) Reverse() *Transaction {
	if tx == nil {
		return NewTransaction()
	}

	postings := Postings(tx.GetPostings()).Reverse()
	ret := NewTransaction().WithPostings(postings...)

	// Copy other fields - copy the MetadataSet reference
	ret.Metadata = tx.GetMetadata()
	if tx.GetTimestamp() != nil {
		ret.Timestamp = tx.GetTimestamp()
	}

	ret.Reference = tx.GetReference()
	ret.Id = tx.GetId()

	ret.Reverted = tx.GetReverted()
	if tx.GetInsertedAt() != nil {
		ret.InsertedAt = tx.GetInsertedAt()
	}

	if tx.GetUpdatedAt() != nil {
		ret.UpdatedAt = tx.GetUpdatedAt()
	}

	if tx.GetRevertedAt() != nil {
		ret.RevertedAt = tx.GetRevertedAt()
	}

	return ret
}

// InvolvedDestinations returns a map of destination accounts to their assets.
func (tx *Transaction) InvolvedDestinations() map[string][]string {
	ret := make(map[string][]string)

	for _, posting := range tx.GetPostings() {
		if posting != nil {
			ret[posting.GetDestination()] = append(ret[posting.GetDestination()], posting.GetAsset())
		}
	}

	for account, assets := range ret {
		sort.Strings(assets)
		ret[account] = slices.Compact(assets)
	}

	return ret
}

// InvolvedAccounts returns a list of all accounts involved in the transaction.
func (tx *Transaction) InvolvedAccounts() []string {
	ret := make([]string, 0)

	for _, posting := range tx.GetPostings() {
		if posting != nil {
			ret = append(ret, posting.GetSource(), posting.GetDestination())
		}
	}

	sort.Strings(ret)

	return slices.Compact(ret)
}

// MarshalJSON implements json.Marshaler for Transaction.
func (tx *Transaction) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Postings   []*Posting     `json:"postings"`
		Metadata   map[string]any `json:"metadata"`
		Timestamp  *time.Time     `json:"timestamp,omitempty"`
		Reference  string         `json:"reference,omitempty"`
		ID         *uint64        `json:"id,omitempty"`
		InsertedAt *time.Time     `json:"insertedAt,omitempty"`
		UpdatedAt  *time.Time     `json:"updatedAt,omitempty"`
		RevertedAt *time.Time     `json:"revertedAt,omitempty"`
		Reverted   bool           `json:"reverted"`
	}

	aux := Aux{
		Postings:  tx.GetPostings(),
		Metadata:  MetadataSetToAnyMap(tx.GetMetadata()),
		Reference: tx.GetReference(),
		Reverted:  tx.IsReverted(),
	}

	if tx.GetId() != 0 {
		aux.ID = new(tx.GetId())
	}

	if tx.GetTimestamp() != nil {
		t := tx.GetTimestamp().AsTime()
		aux.Timestamp = &t
	}

	if tx.GetInsertedAt() != nil {
		t := tx.GetInsertedAt().AsTime()
		aux.InsertedAt = &t
	}

	if tx.GetUpdatedAt() != nil {
		t := tx.GetUpdatedAt().AsTime()
		aux.UpdatedAt = &t
	}

	if tx.GetRevertedAt() != nil {
		t := tx.GetRevertedAt().AsTime()
		aux.RevertedAt = &t
	}

	return json.Marshal(aux)
}
