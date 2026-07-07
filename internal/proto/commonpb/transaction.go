package commonpb

import (
	"slices"
	"sort"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// NewTransaction creates a new Transaction with empty metadata.
func NewTransaction() *Transaction {
	return &Transaction{
		Metadata: map[string]*MetadataValue{},
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

	tx.Timestamp = uint64(NewTimestamp(ts))

	return tx
}

// WithMetadata sets the metadata of the transaction.
func (tx *Transaction) WithMetadata(m metadata.Metadata) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.Metadata = MetadataFromGoMap(m)

	return tx
}

// WithInsertedAt sets the inserted_at timestamp.
func (tx *Transaction) WithInsertedAt(date time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.InsertedAt = uint64(NewTimestamp(date))

	return tx
}

// WithUpdatedAt sets the updated_at timestamp.
func (tx *Transaction) WithUpdatedAt(at time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.UpdatedAt = uint64(NewTimestamp(at))

	return tx
}

// WithRevertedAt sets the reverted_at timestamp and marks the transaction as reverted.
func (tx *Transaction) WithRevertedAt(timestamp time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}

	tx.RevertedAt = uint64(NewTimestamp(timestamp))
	tx.Reverted = true

	return tx
}

// IsReverted returns true if the transaction is reverted.
func (tx *Transaction) IsReverted() bool {
	if tx == nil {
		return false
	}

	return tx.GetReverted() || tx.GetRevertedAt() != 0
}

// Reverse creates a reversed copy of the transaction with swapped source/destination in postings.
func (tx *Transaction) Reverse() *Transaction {
	if tx == nil {
		return NewTransaction()
	}

	postings := Postings(tx.GetPostings()).Reverse()
	ret := NewTransaction().WithPostings(postings...)

	// Copy other fields - copy the metadata map reference
	ret.Metadata = tx.GetMetadata()
	ret.Timestamp = tx.GetTimestamp()
	ret.Reference = tx.GetReference()
	ret.Id = tx.GetId()
	ret.Reverted = tx.GetReverted()
	ret.InsertedAt = tx.GetInsertedAt()
	ret.UpdatedAt = tx.GetUpdatedAt()
	ret.RevertedAt = tx.GetRevertedAt()

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
		Metadata:  MetadataToAnyMap(tx.GetMetadata()),
		Reference: tx.GetReference(),
		Reverted:  tx.IsReverted(),
	}

	if tx.GetId() != 0 {
		aux.ID = new(tx.GetId())
	}

	if ts := tx.TimestampTs(); ts.IsSet() {
		t := ts.AsTime()
		aux.Timestamp = &t
	}

	if ts := tx.InsertedAtTs(); ts.IsSet() {
		t := ts.AsTime()
		aux.InsertedAt = &t
	}

	if ts := tx.UpdatedAtTs(); ts.IsSet() {
		t := ts.AsTime()
		aux.UpdatedAt = &t
	}

	if ts := tx.RevertedAtTs(); ts.IsSet() {
		t := ts.AsTime()
		aux.RevertedAt = &t
	}

	return json.Marshal(aux)
}

// UnmarshalJSON implements json.Unmarshaler for Transaction. Mirrors
// MarshalJSON: RFC3339 timestamp strings ↔ fixed64 micros fields. Necessary
// because after inlining Timestamp as a scalar, the default decoder would
// try to parse `"2020-06-15T12:00:00Z"` as a uint64 and fail — breaking every
// round-trip that passes through Transaction JSON (notably HydrateLog for
// CreatedTransaction / RevertedTransaction log payloads).
func (tx *Transaction) UnmarshalJSON(data []byte) error {
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

	var aux Aux
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	tx.Postings = aux.Postings
	tx.Reference = aux.Reference
	tx.Reverted = aux.Reverted

	if aux.ID != nil {
		tx.Id = *aux.ID
	}

	if md, err := MetadataFromAnyMap(aux.Metadata); err != nil {
		return err
	} else {
		tx.Metadata = md
	}

	setFromTimePtr := func(dst *uint64, src *time.Time) error {
		if src == nil {
			*dst = 0

			return nil
		}
		if src.UnixMicro() <= 0 {
			return ErrTimestampBeforeEpoch
		}
		*dst = uint64(src.UnixMicro())

		return nil
	}

	if err := setFromTimePtr(&tx.Timestamp, aux.Timestamp); err != nil {
		return err
	}
	if err := setFromTimePtr(&tx.InsertedAt, aux.InsertedAt); err != nil {
		return err
	}
	if err := setFromTimePtr(&tx.UpdatedAt, aux.UpdatedAt); err != nil {
		return err
	}
	if err := setFromTimePtr(&tx.RevertedAt, aux.RevertedAt); err != nil {
		return err
	}

	return nil
}
