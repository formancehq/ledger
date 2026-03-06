package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// analyzeTransactionsResponseJSON is the camelCase JSON DTO for AnalyzeTransactionsResponse.
type analyzeTransactionsResponseJSON struct {
	FlowPatterns      []*flowPatternJSON `json:"flowPatterns"`
	TotalTransactions uint64             `json:"totalTransactions"`
	TotalReverted     uint64             `json:"totalReverted"`
}

type flowPatternJSON struct {
	Signature        string                   `json:"signature"`
	Structure        string                   `json:"structure"`
	TransactionCount uint64                   `json:"transactionCount"`
	Postings         []*normalizedPostingJSON `json:"postings"`
	Temporal         *temporalStatsJSON       `json:"temporal,omitempty"`
	VolumeStats      []*assetVolumeStatsJSON  `json:"volumeStats"`
	MetadataKeys     []string                 `json:"metadataKeys"`
}

type normalizedPostingJSON struct {
	SourcePattern      string `json:"sourcePattern"`
	DestinationPattern string `json:"destinationPattern"`
	Asset              string `json:"asset"`
}

type temporalStatsJSON struct {
	FirstSeen          string            `json:"firstSeen,omitempty"`
	LastSeen           string            `json:"lastSeen,omitempty"`
	TransactionsPerDay float64           `json:"transactionsPerDay"`
	PeakHours          []*hourBucketJSON `json:"peakHours,omitempty"`
}

type hourBucketJSON struct {
	Hour  uint32 `json:"hour"`
	Count uint64 `json:"count"`
}

type assetVolumeStatsJSON struct {
	Asset            string `json:"asset"`
	TotalVolume      string `json:"totalVolume"`
	AverageVolume    string `json:"averageVolume"`
	MinVolume        string `json:"minVolume"`
	MaxVolume        string `json:"maxVolume"`
	TransactionCount uint64 `json:"transactionCount"`
}

func postingStructureToString(s servicepb.PostingStructure) string {
	switch s {
	case servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE:
		return "simple"
	case servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_SOURCE:
		return "multiSource"
	case servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION:
		return "multiDestination"
	case servicepb.PostingStructure_POSTING_STRUCTURE_COMPLEX:
		return "complex"
	default:
		return "unknown"
	}
}

func toAnalyzeTransactionsJSON(resp *servicepb.AnalyzeTransactionsResponse) *analyzeTransactionsResponseJSON {
	result := &analyzeTransactionsResponseJSON{
		TotalTransactions: resp.GetTotalTransactions(),
		TotalReverted:     resp.GetTotalReverted(),
		FlowPatterns:      make([]*flowPatternJSON, 0, len(resp.GetFlowPatterns())),
	}

	for _, fp := range resp.GetFlowPatterns() {
		result.FlowPatterns = append(result.FlowPatterns, toFlowPatternJSON(fp))
	}

	return result
}

func toFlowPatternJSON(fp *servicepb.FlowPattern) *flowPatternJSON {
	result := &flowPatternJSON{
		Signature:        fp.GetSignature(),
		Structure:        postingStructureToString(fp.GetStructure()),
		TransactionCount: fp.GetTransactionCount(),
		MetadataKeys:     fp.GetMetadataKeys(),
	}

	result.Postings = make([]*normalizedPostingJSON, 0, len(fp.GetPostings()))
	for _, p := range fp.GetPostings() {
		result.Postings = append(result.Postings, &normalizedPostingJSON{
			SourcePattern:      p.GetSourcePattern(),
			DestinationPattern: p.GetDestinationPattern(),
			Asset:              p.GetAsset(),
		})
	}

	if fp.GetTemporal() != nil {
		result.Temporal = &temporalStatsJSON{
			TransactionsPerDay: fp.GetTemporal().GetTransactionsPerDay(),
		}
		if fp.GetTemporal().GetFirstSeen() != nil {
			result.Temporal.FirstSeen = fp.GetTemporal().GetFirstSeen().AsTime().Format("2006-01-02T15:04:05Z07:00")
		}

		if fp.GetTemporal().GetLastSeen() != nil {
			result.Temporal.LastSeen = fp.GetTemporal().GetLastSeen().AsTime().Format("2006-01-02T15:04:05Z07:00")
		}

		for _, h := range fp.GetTemporal().GetPeakHours() {
			result.Temporal.PeakHours = append(result.Temporal.PeakHours, &hourBucketJSON{
				Hour:  h.GetHour(),
				Count: h.GetCount(),
			})
		}
	}

	result.VolumeStats = make([]*assetVolumeStatsJSON, 0, len(fp.GetVolumeStats()))
	for _, vs := range fp.GetVolumeStats() {
		result.VolumeStats = append(result.VolumeStats, &assetVolumeStatsJSON{
			Asset:            vs.GetAsset(),
			TotalVolume:      vs.GetTotalVolume(),
			AverageVolume:    vs.GetAverageVolume(),
			MinVolume:        vs.GetMinVolume(),
			MaxVolume:        vs.GetMaxVolume(),
			TransactionCount: vs.GetTransactionCount(),
		})
	}

	return result
}

// handleAnalyzeTransactions handles GET /{ledgerName}/analyze-transactions.
func (s *Server) handleAnalyzeTransactions(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))

		return
	}

	var variableThreshold uint32

	if v := r.URL.Query().Get("variableThreshold"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("variableThreshold must be a positive integer"))

			return
		}

		variableThreshold = uint32(parsed)
	}

	resp, err := s.backend.AnalyzeTransactions(r.Context(), ledgerName, variableThreshold, nil)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, toAnalyzeTransactionsJSON(resp))
}
