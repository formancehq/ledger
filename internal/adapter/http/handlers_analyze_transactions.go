package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
	// Color is always emitted (even when empty) so REST clients can
	// distinguish patterns that differ only by color bucket. Two flows
	// with the same (source, destination, asset) but different colors
	// surface here as distinct rows.
	Color string `json:"color"`
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
	Asset            string            `json:"asset"`
	TotalVolume      *commonpb.BigUint `json:"totalVolume"`
	AverageVolume    *commonpb.BigUint `json:"averageVolume"`
	MinVolume        *commonpb.BigUint `json:"minVolume"`
	MaxVolume        *commonpb.BigUint `json:"maxVolume"`
	TransactionCount uint64            `json:"transactionCount"`
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

func toAnalyzeTransactionsJSON(resp *servicepb.AnalyzeTransactionsResponse) (*analyzeTransactionsResponseJSON, error) {
	result := &analyzeTransactionsResponseJSON{
		TotalTransactions: resp.GetTotalTransactions(),
		TotalReverted:     resp.GetTotalReverted(),
		FlowPatterns:      make([]*flowPatternJSON, 0, len(resp.GetFlowPatterns())),
	}

	for _, fp := range resp.GetFlowPatterns() {
		pattern, err := toFlowPatternJSON(fp)
		if err != nil {
			return nil, err
		}
		result.FlowPatterns = append(result.FlowPatterns, pattern)
	}

	return result, nil
}

func toFlowPatternJSON(fp *servicepb.FlowPattern) (*flowPatternJSON, error) {
	result := &flowPatternJSON{
		Signature:        fp.GetSignature(),
		Structure:        postingStructureToString(fp.GetStructure()),
		TransactionCount: fp.GetTransactionCount(),
		MetadataKeys:     nonNilStrings(fp.GetMetadataKeys()),
	}

	result.Postings = make([]*normalizedPostingJSON, 0, len(fp.GetPostings()))
	for _, p := range fp.GetPostings() {
		result.Postings = append(result.Postings, &normalizedPostingJSON{
			SourcePattern:      p.GetSourcePattern(),
			DestinationPattern: p.GetDestinationPattern(),
			Asset:              p.GetAsset(),
			Color:              p.GetColor(),
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
		statistics := &assetVolumeStatsJSON{Asset: vs.GetAsset(), TransactionCount: vs.GetTransactionCount()}
		for _, amount := range []struct {
			decimal string
			target  **commonpb.BigUint
		}{
			{vs.GetTotalVolume(), &statistics.TotalVolume},
			{vs.GetAverageVolume(), &statistics.AverageVolume},
			{vs.GetMinVolume(), &statistics.MinVolume},
			{vs.GetMaxVolume(), &statistics.MaxVolume},
		} {
			value := &commonpb.BigUint{}
			if err := value.UnmarshalJSON(strconv.AppendQuote(nil, amount.decimal)); err != nil {
				return nil, fmt.Errorf("invalid analysis volume statistic: %w", err)
			}
			*amount.target = value
		}
		result.VolumeStats = append(result.VolumeStats, statistics)
	}

	return result, nil
}

// handleAnalyzeTransactions handles GET /{ledgerName}/analyze-transactions.
func (s *Server) handleAnalyzeTransactions(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
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

	body, err := toAnalyzeTransactionsJSON(resp)
	if err != nil {
		handleError(w, r, err)

		return
	}
	writeMonetaryOK(w, r, body)
}
