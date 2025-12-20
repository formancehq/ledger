package ledger

import (
	"encoding/json"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
)

// LedgerInfo represents information about a ledger
type LedgerInfo struct {
	ID                uint64            `json:"id"`                // Sequential ID for the ledger
	Name              string            `json:"name"`              // Ledger name/ID
	Driver            string            `json:"driver"`            // Driver name (e.g., "sqlite", "s3", etc.)
	Config            json.RawMessage    `json:"config"`            // Driver-specific configuration
	CreatedAt         time.Time         `json:"createdAt"`         // Creation timestamp
	Metadata          metadata.Metadata `json:"metadata,omitempty"`
	SnapshotThreshold uint64            `json:"snapshotThreshold"` // Number of logs before triggering a snapshot (0 means use global config)
}
