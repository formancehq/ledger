package grpc

import (
	"time"

	"github.com/formancehq/ledger/pkg/client/v3/internal/json"
)

// MarshalJSON implements json.Marshaler for Transaction.
func (tx *Transaction) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Postings                []*Posting         `json:"postings"`
		Metadata                map[string]any     `json:"metadata"`
		Timestamp               *time.Time         `json:"timestamp,omitempty"`
		Reference               string             `json:"reference,omitempty"`
		ID                      uint64             `json:"id"`
		InsertedAt              *time.Time         `json:"insertedAt,omitempty"`
		UpdatedAt               *time.Time         `json:"updatedAt,omitempty"`
		RevertedAt              *time.Time         `json:"revertedAt,omitempty"`
		RevertedByTransactionID uint64             `json:"revertedByTransactionId,omitempty"`
		RevertsTransactionID    uint64             `json:"revertsTransactionId,omitempty"`
		Reverted                bool               `json:"reverted"`
		PostCommitVolumes       *PostCommitVolumes `json:"postCommitVolumes,omitempty"`
	}

	// Collections are emitted unconditionally and must never be null: the
	// OpenAPI schema types them as non-nullable and lists them in `required`.
	// GetPostings() and MetadataToAnyMap() both return nil for an absent value,
	// so normalise here rather than in MetadataToAnyMap, which has 7 non-test
	// callers (this one included) whose payloads must not change.
	postings := tx.GetPostings()
	if postings == nil {
		postings = []*Posting{}
	}

	metadataMap := MetadataToAnyMap(tx.GetMetadata())
	if metadataMap == nil {
		metadataMap = map[string]any{}
	}

	aux := Aux{
		Postings:                postings,
		Metadata:                metadataMap,
		Reference:               tx.GetReference(),
		ID:                      tx.GetId(),
		Reverted:                (tx.GetReverted() || tx.GetRevertedAt() != nil),
		RevertedByTransactionID: tx.GetRevertedByTransaction(),
		RevertsTransactionID:    tx.GetRevertsTransaction(),
		PostCommitVolumes:       tx.GetPostCommitVolumes(),
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
