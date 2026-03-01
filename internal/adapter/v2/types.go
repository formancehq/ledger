package v2

import (
	"encoding/json"
)

// V2LogPage represents a paginated response from the v2 logs endpoint.
type V2LogPage struct {
	Cursor V2LogCursor `json:"cursor"`
}

// V2LogCursor holds pagination data for v2 logs.
type V2LogCursor struct {
	PageSize int     `json:"pageSize"`
	HasMore  bool    `json:"hasMore"`
	Data     []V2Log `json:"data"`
}

// V2Log represents a single log entry from the v2 API.
type V2Log struct {
	ID   uint64          `json:"id"`
	Type string          `json:"type"`
	Date string          `json:"date"`
	Data json.RawMessage `json:"data"`
	Hash string          `json:"hash"`
}

// V2NewTransactionData represents the data for a NEW_TRANSACTION log.
type V2NewTransactionData struct {
	Transaction     V2Transaction             `json:"transaction"`
	AccountMetadata map[string]map[string]any `json:"accountMetadata,omitempty"`
}

// V2Transaction represents a v2 transaction.
type V2Transaction struct {
	ID        uint64         `json:"id"`
	Postings  []V2Posting    `json:"postings"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Timestamp string         `json:"timestamp"`
	Reference string         `json:"reference,omitempty"`
	Reverted  bool           `json:"reverted,omitempty"`
}

// V2Posting represents a v2 posting.
type V2Posting struct {
	Source      string      `json:"source"`
	Destination string      `json:"destination"`
	Amount      json.Number `json:"amount"` // v2 stores as JSON number; json.Number handles both number and string
	Asset       string      `json:"asset"`
}

// V2SetMetadataData represents the data for a SET_METADATA log.
type V2SetMetadataData struct {
	TargetType string          `json:"targetType"`
	TargetID   json.RawMessage `json:"targetId"`
	Metadata   map[string]any  `json:"metadata"`
}

// V2RevertedTransactionData represents the data for a REVERTED_TRANSACTION log.
type V2RevertedTransactionData struct {
	RevertedTransactionID uint64        `json:"revertedTransactionID"`
	RevertTransaction     V2Transaction `json:"transaction"`
}

// V2DeleteMetadataData represents the data for a DELETE_METADATA log.
type V2DeleteMetadataData struct {
	TargetType string          `json:"targetType"`
	TargetID   json.RawMessage `json:"targetId"`
	Key        string          `json:"key"`
}
