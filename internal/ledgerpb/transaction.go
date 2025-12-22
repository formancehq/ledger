package ledgerpb

import (
	"encoding/json"
	"math/big"
	"slices"
	"sort"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"
	"google.golang.org/protobuf/types/known/structpb"
)

// NewTransactionData creates a new TransactionData with empty metadata
func NewTransactionData() *TransactionData {
	return &TransactionData{
		Metadata: make(map[string]string),
	}
}

// WithPostings adds postings to TransactionData
func (td *TransactionData) WithPostings(postings ...*Posting) *TransactionData {
	if td == nil {
		td = NewTransactionData()
	}
	td.Postings = append(td.Postings, postings...)
	return td
}

// NewTransaction creates a new Transaction with empty TransactionData
func NewTransaction() *Transaction {
	return &Transaction{
		Metadata: make(map[string]string),
	}
}

// WithPostings adds postings to Transaction
func (tx *Transaction) WithPostings(postings ...*Posting) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Postings = append(tx.Postings, postings...)
	return tx
}

// WithID sets the ID of the transaction
func (tx *Transaction) WithID(id uint64) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Id = id
	return tx
}

// WithReference sets the reference of the transaction
func (tx *Transaction) WithReference(ref string) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Reference = ref
	return tx
}

// WithTimestamp sets the timestamp of the transaction
func (tx *Transaction) WithTimestamp(ts time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Timestamp = NewTimestamp(ts)
	return tx
}

// WithMetadata sets the metadata of the transaction
func (tx *Transaction) WithMetadata(m metadata.Metadata) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.Metadata = m
	return tx
}

// WithInsertedAt sets the inserted_at timestamp
func (tx *Transaction) WithInsertedAt(date time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.InsertedAt = NewTimestamp(date)
	return tx
}

// WithUpdatedAt sets the updated_at timestamp
func (tx *Transaction) WithUpdatedAt(at time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.UpdatedAt = NewTimestamp(at)
	return tx
}

// WithRevertedAt sets the reverted_at timestamp and marks the transaction as reverted
func (tx *Transaction) WithRevertedAt(timestamp time.Time) *Transaction {
	if tx == nil {
		tx = NewTransaction()
	}
	tx.RevertedAt = NewTimestamp(timestamp)
	tx.Reverted = true
	return tx
}

// IsReverted returns true if the transaction is reverted
func (tx *Transaction) IsReverted() bool {
	if tx == nil {
		return false
	}
	return tx.Reverted || tx.RevertedAt != nil
}

// Reverse creates a reversed copy of the transaction with swapped source/destination in postings
func (tx *Transaction) Reverse() *Transaction {
	if tx == nil {
		return NewTransaction()
	}
	postings := Postings(tx.Postings).Reverse()
	ret := NewTransaction().WithPostings(postings...)

	// Copy other fields
	if tx.Metadata != nil {
		ret.Metadata = tx.Metadata
	}
	if tx.Timestamp != nil {
		ret.Timestamp = tx.Timestamp
	}
	ret.Reference = tx.Reference
	ret.Id = tx.Id
	ret.Reverted = tx.Reverted
	if tx.InsertedAt != nil {
		ret.InsertedAt = tx.InsertedAt
	}
	if tx.UpdatedAt != nil {
		ret.UpdatedAt = tx.UpdatedAt
	}
	if tx.RevertedAt != nil {
		ret.RevertedAt = tx.RevertedAt
	}

	return ret
}

// InvolvedDestinations returns a map of destination accounts to their assets
func (tx *Transaction) InvolvedDestinations() map[string][]string {
	ret := make(map[string][]string)
	for _, posting := range tx.Postings {
		if posting != nil {
			ret[posting.Destination] = append(ret[posting.Destination], posting.Asset)
		}
	}

	for account, assets := range ret {
		sort.Strings(assets)
		ret[account] = slices.Compact(assets)
	}

	return ret
}

// InvolvedAccounts returns a list of all accounts involved in the transaction
func (tx *Transaction) InvolvedAccounts() []string {
	ret := make([]string, 0)
	for _, posting := range tx.Postings {
		if posting != nil {
			ret = append(ret, posting.Source, posting.Destination)
		}
	}

	sort.Strings(ret)

	return slices.Compact(ret)
}

// VolumeUpdates calculates volume updates for all accounts and assets
func (tx *Transaction) VolumeUpdates() []*AccountsVolumes {
	aggregatedVolumes := make(map[string]map[string][]*Posting)
	for _, posting := range tx.Postings {
		if posting == nil {
			continue
		}
		if _, ok := aggregatedVolumes[posting.Source]; !ok {
			aggregatedVolumes[posting.Source] = make(map[string][]*Posting)
		}
		aggregatedVolumes[posting.Source][posting.Asset] = append(aggregatedVolumes[posting.Source][posting.Asset], posting)

		if posting.Source == posting.Destination {
			continue
		}

		if _, ok := aggregatedVolumes[posting.Destination]; !ok {
			aggregatedVolumes[posting.Destination] = make(map[string][]*Posting)
		}
		aggregatedVolumes[posting.Destination][posting.Asset] = append(aggregatedVolumes[posting.Destination][posting.Asset], posting)
	}

	ret := make([]*AccountsVolumes, 0)
	for account, movesByAsset := range aggregatedVolumes {
		for asset, postings := range movesByAsset {
			input := big.NewInt(0)
			output := big.NewInt(0)
			for _, posting := range postings {
				if posting == nil {
					continue
				}
				if account == posting.Source {
					output.Add(output, posting.Amount.Value())
				}
				if account == posting.Destination {
					input.Add(input, posting.Amount.Value())
				}
			}

			ret = append(ret, &AccountsVolumes{
				Account: account,
				Asset:   asset,
				Input:   input.String(),
				Output:  output.String(),
			})
		}
	}

	slices.SortStableFunc(ret, func(a, b *AccountsVolumes) int {
		switch {
		case a.Account < b.Account:
			return -1
		case a.Account > b.Account:
			return 1
		default:
			switch {
			case a.Asset < b.Asset:
				return -1
			case a.Asset > b.Asset:
				return 1
			default:
				return 0
			}
		}
	})

	return ret
}

// MetadataToStruct converts metadata.Metadata to *structpb.Struct
func MetadataToStruct(md metadata.Metadata) (*structpb.Struct, error) {
	if len(md) == 0 {
		return &structpb.Struct{Fields: make(map[string]*structpb.Value)}, nil
	}
	fields := make(map[string]*structpb.Value)
	for k, v := range md {
		val, err := structpb.NewValue(v)
		if err != nil {
			return nil, err
		}
		fields[k] = val
	}
	return &structpb.Struct{Fields: fields}, nil
}

// StructToMetadata converts *structpb.Struct to metadata.Metadata
func StructToMetadata(s *structpb.Struct) metadata.Metadata {
	if s == nil {
		return metadata.Metadata{}
	}
	md := make(metadata.Metadata)
	for k, v := range s.Fields {
		md[k] = v.GetStringValue()
	}
	return md
}

// MarshalJSON implements json.Marshaler for Transaction
func (tx *Transaction) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Postings   []*Posting        `json:"postings"`
		Metadata   metadata.Metadata `json:"metadata"`
		Timestamp  *time.Time        `json:"timestamp,omitempty"`
		Reference  string            `json:"reference,omitempty"`
		ID         *uint64           `json:"id,omitempty"`
		InsertedAt *time.Time        `json:"insertedAt,omitempty"`
		UpdatedAt  *time.Time        `json:"updatedAt,omitempty"`
		RevertedAt *time.Time        `json:"revertedAt,omitempty"`
		Reverted   bool              `json:"reverted"`
	}

	aux := Aux{
		Postings:  tx.Postings,
		Metadata:  tx.Metadata,
		Reference: tx.Reference,
		Reverted:  tx.IsReverted(),
	}

	if tx.Id != 0 {
		aux.ID = pointer.For(tx.Id)
	}
	if tx.Timestamp != nil {
		t := tx.Timestamp.AsTime()
		aux.Timestamp = &t
	}
	if tx.InsertedAt != nil {
		t := tx.InsertedAt.AsTime()
		aux.InsertedAt = &t
	}
	if tx.UpdatedAt != nil {
		t := tx.UpdatedAt.AsTime()
		aux.UpdatedAt = &t
	}
	if tx.RevertedAt != nil {
		t := tx.RevertedAt.AsTime()
		aux.RevertedAt = &t
	}

	return json.Marshal(aux)
}
