package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// analyzeTransactionsResponseJSON is the camelCase JSON DTO for AnalyzeTransactionsResponse.
type analyzeTransactionsResponseJSON struct {
	FlowPatterns      []*flowPatternJSON `json:"flowPatterns"`
	TotalTransactions uint64             `json:"totalTransactions"`
	TotalReverted     uint64             `json:"totalReverted"`
}

type flowPatternJSON struct {
	Signature        string                  `json:"signature"`
	Structure        string                  `json:"structure"`
	TransactionCount uint64                  `json:"transactionCount"`
	Postings         []*normalizedPostingJSON `json:"postings"`
	Temporal         *temporalStatsJSON      `json:"temporal,omitempty"`
	VolumeStats      []*assetVolumeStatsJSON `json:"volumeStats"`
	MetadataKeys     []string                `json:"metadataKeys"`
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
		TotalTransactions: resp.TotalTransactions,
		TotalReverted:     resp.TotalReverted,
		FlowPatterns:      make([]*flowPatternJSON, 0, len(resp.FlowPatterns)),
	}

	for _, fp := range resp.FlowPatterns {
		result.FlowPatterns = append(result.FlowPatterns, toFlowPatternJSON(fp))
	}

	return result
}

func toFlowPatternJSON(fp *servicepb.FlowPattern) *flowPatternJSON {
	result := &flowPatternJSON{
		Signature:        fp.Signature,
		Structure:        postingStructureToString(fp.Structure),
		TransactionCount: fp.TransactionCount,
		MetadataKeys:     fp.MetadataKeys,
	}

	result.Postings = make([]*normalizedPostingJSON, 0, len(fp.Postings))
	for _, p := range fp.Postings {
		result.Postings = append(result.Postings, &normalizedPostingJSON{
			SourcePattern:      p.SourcePattern,
			DestinationPattern: p.DestinationPattern,
			Asset:              p.Asset,
		})
	}

	if fp.Temporal != nil {
		result.Temporal = &temporalStatsJSON{
			TransactionsPerDay: fp.Temporal.TransactionsPerDay,
		}
		if fp.Temporal.FirstSeen != nil {
			result.Temporal.FirstSeen = fp.Temporal.FirstSeen.AsTime().Format("2006-01-02T15:04:05Z07:00")
		}
		if fp.Temporal.LastSeen != nil {
			result.Temporal.LastSeen = fp.Temporal.LastSeen.AsTime().Format("2006-01-02T15:04:05Z07:00")
		}
		for _, h := range fp.Temporal.PeakHours {
			result.Temporal.PeakHours = append(result.Temporal.PeakHours, &hourBucketJSON{
				Hour:  h.Hour,
				Count: h.Count,
			})
		}
	}

	result.VolumeStats = make([]*assetVolumeStatsJSON, 0, len(fp.VolumeStats))
	for _, vs := range fp.VolumeStats {
		result.VolumeStats = append(result.VolumeStats, &assetVolumeStatsJSON{
			Asset:            vs.Asset,
			TotalVolume:      vs.TotalVolume,
			AverageVolume:    vs.AverageVolume,
			MinVolume:        vs.MinVolume,
			MaxVolume:        vs.MaxVolume,
			TransactionCount: vs.TransactionCount,
		})
	}

	return result
}

// handleAnalyzeTransactions handles GET /{ledgerName}/analyze-transactions
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
