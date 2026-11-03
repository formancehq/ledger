package servicepb

import (
	"encoding/json"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// MarshalJSON implements the public REST shape for an index status row.
func (x *IndexEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Ledger         string          `json:"ledger"`
		Index          *commonpb.Index `json:"index,omitempty"`
		Cursor         uint64          `json:"cursor"`
		CurrentVersion uint32          `json:"currentVersion"`
		PendingVersion uint32          `json:"pendingVersion"`
	}{
		Ledger: x.GetLedger(), Index: x.GetIndex(), Cursor: x.GetCursor(),
		CurrentVersion: x.GetCurrentVersion(), PendingVersion: x.GetPendingVersion(),
	})
}

// MarshalJSON implements the public REST shape for aggregate index status.
func (x *GetIndexStatusResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		LastIndexedSequence uint64        `json:"lastIndexedSequence"`
		LastLogSequence     uint64        `json:"lastLogSequence"`
		Lag                 uint64        `json:"lag"`
		IndexFileSize       uint64        `json:"indexFileSize"`
		Indexes             []*IndexEntry `json:"indexes"`
	}{
		LastIndexedSequence: x.GetLastIndexedSequence(), LastLogSequence: x.GetLastLogSequence(),
		Lag: x.GetLag(), IndexFileSize: x.GetIndexFileSize(), Indexes: x.GetIndexes(),
	})
}
