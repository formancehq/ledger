package ledger

import (
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
)

// LedgerInfo represents information about a ledger
type LedgerInfo struct {
	ID        uint64            `json:"id"`        // Sequential ID for the ledger
	Name      string            `json:"name"`      // Ledger name/ID
	CreatedAt time.Time         `json:"createdAt"` // Creation timestamp
	Metadata  metadata.Metadata `json:"metadata,omitempty"`
	LastLogID *uint64           `json:"lastLogId,omitempty"` // ID of the last log for this ledger
}

// BucketInfo represents information about a bucket
type BucketInfo struct {
	ID        uint64                 `json:"id"`        // Sequential bucket ID
	Name      string                 `json:"name"`      // Bucket name/ID
	Driver    string                 `json:"driver"`    // Driver name (e.g., "postgres", "s3", etc.)
	Config    map[string]interface{} `json:"config"`    // Driver-specific configuration
	CreatedAt time.Time              `json:"createdAt"` // Creation timestamp
}
