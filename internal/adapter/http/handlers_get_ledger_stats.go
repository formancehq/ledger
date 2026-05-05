package http

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// ledgerStatsJSON is the camelCase JSON DTO for LedgerStats.
type ledgerStatsJSON struct {
	TransactionCount        uint64 `json:"transactionCount"`
	VolumeCount             uint64 `json:"volumeCount"`
	MetadataCount           uint64 `json:"metadataCount"`
	ReferenceCount          uint64 `json:"referenceCount"`
	PostingCount            uint64 `json:"postingCount"`
	LogCount                uint64 `json:"logCount"`
	EphemeralEvictedCount   uint64 `json:"ephemeralEvictedCount"`
	TransientUsedCount      uint64 `json:"transientUsedCount"`
	RevertCount             uint64 `json:"revertCount"`
	NumscriptExecutionCount uint64 `json:"numscriptExecutionCount"`
}

func toLedgerStatsJSON(stats *commonpb.LedgerStats) *ledgerStatsJSON {
	return &ledgerStatsJSON{
		TransactionCount:        stats.GetTransactionCount(),
		VolumeCount:             stats.GetVolumeCount(),
		MetadataCount:           stats.GetMetadataCount(),
		ReferenceCount:          stats.GetReferenceCount(),
		PostingCount:            stats.GetPostingCount(),
		LogCount:                stats.GetLogCount(),
		EphemeralEvictedCount:   stats.GetEphemeralEvictedCount(),
		TransientUsedCount:      stats.GetTransientUsedCount(),
		RevertCount:             stats.GetRevertCount(),
		NumscriptExecutionCount: stats.GetNumscriptExecutionCount(),
	}
}

// handleGetLedgerStats handles GET /{ledgerName}/stats to retrieve ledger statistics.
func (s *Server) handleGetLedgerStats(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	stats, err := s.backend.GetLedgerStats(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, toLedgerStatsJSON(stats))
}
