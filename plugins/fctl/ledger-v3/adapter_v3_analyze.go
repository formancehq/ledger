package ledgerv3

import (
	"encoding/json"
	"time"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// The analysis HTTP endpoints expose deliberately typed JSON DTOs rather than
// protobuf JSON. Keep the portable command output on that public product shape:
// fixed64 counters remain JSON numbers, enum values use their documented names,
// timestamps are RFC 3339 strings, and repeated fields are never null.
type analyzeAccountsJSON struct {
	Patterns      []*accountPatternJSON `json:"patterns"`
	TotalAccounts uint64                `json:"totalAccounts"`
}

type accountPatternJSON struct {
	Pattern      string                `json:"pattern"`
	AccountCount uint64                `json:"accountCount"`
	Assets       []string              `json:"assets"`
	MetadataKeys []string              `json:"metadataKeys"`
	Segments     []*patternSegmentJSON `json:"segments"`
}

type patternSegmentJSON struct {
	Position        uint32   `json:"position"`
	Type            string   `json:"type"`
	FixedValue      string   `json:"fixedValue,omitempty"`
	VariableName    string   `json:"variableName,omitempty"`
	InferredPattern string   `json:"inferredPattern,omitempty"`
	UniqueValues    uint64   `json:"uniqueValues"`
	Examples        []string `json:"examples"`
}

type analyzeTransactionsJSON struct {
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
	Color              string `json:"color"`
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

type analyzeProgressJSON struct {
	Processed uint64 `json:"processed"`
	Total     uint64 `json:"total"`
	Phase     string `json:"phase"`
}

func emitAnalyzeProgress(host sdk.Host, progress *servicepb.AnalyzeProgress) error {
	return emitJSONProgress(host, analyzeProgressJSON{
		Processed: progress.GetProcessed(),
		Total:     progress.GetTotal(),
		Phase:     progress.GetPhase(),
	})
}

func emitJSONProgress(host sdk.Host, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return v3Failure("encode analysis progress: %v", err)
	}
	return host.Emit(sdk.Event{Kind: sdk.EventProgress, Payload: encoded})
}

func emitAnalyzeAccountsResult(host sdk.Host, operationID string, response *servicepb.AnalyzeAccountsResponse) error {
	result := &analyzeAccountsJSON{
		Patterns:      make([]*accountPatternJSON, 0, len(response.GetPatterns())),
		TotalAccounts: response.GetTotalAccounts(),
	}
	for _, pattern := range response.GetPatterns() {
		item := &accountPatternJSON{
			Pattern: pattern.GetPattern(), AccountCount: pattern.GetAccountCount(),
			Assets: nonNilStrings(pattern.GetAssets()), MetadataKeys: nonNilStrings(pattern.GetMetadataKeys()),
			Segments: make([]*patternSegmentJSON, 0, len(pattern.GetSegments())),
		}
		for _, segment := range pattern.GetSegments() {
			segmentType := "fixed"
			if segment.GetType() == servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE {
				segmentType = "variable"
			}
			item.Segments = append(item.Segments, &patternSegmentJSON{
				Position: segment.GetPosition(), Type: segmentType, FixedValue: segment.GetFixedValue(),
				VariableName: segment.GetVariableName(), InferredPattern: segment.GetInferredPattern(),
				UniqueValues: segment.GetUniqueValues(), Examples: nonNilStrings(segment.GetExamples()),
			})
		}
		result.Patterns = append(result.Patterns, item)
	}
	return emitJSON(host, operationID, sdk.ResultObject, result, nil)
}

func emitAnalyzeTransactionsResult(host sdk.Host, operationID string, response *servicepb.AnalyzeTransactionsResponse) error {
	result := &analyzeTransactionsJSON{
		FlowPatterns:      make([]*flowPatternJSON, 0, len(response.GetFlowPatterns())),
		TotalTransactions: response.GetTotalTransactions(), TotalReverted: response.GetTotalReverted(),
	}
	for _, pattern := range response.GetFlowPatterns() {
		item := &flowPatternJSON{
			Signature: pattern.GetSignature(), Structure: postingStructureName(pattern.GetStructure()),
			TransactionCount: pattern.GetTransactionCount(),
			Postings:         make([]*normalizedPostingJSON, 0, len(pattern.GetPostings())),
			VolumeStats:      make([]*assetVolumeStatsJSON, 0, len(pattern.GetVolumeStats())),
			MetadataKeys:     nonNilStrings(pattern.GetMetadataKeys()),
		}
		for _, posting := range pattern.GetPostings() {
			item.Postings = append(item.Postings, &normalizedPostingJSON{
				SourcePattern: posting.GetSourcePattern(), DestinationPattern: posting.GetDestinationPattern(),
				Asset: posting.GetAsset(), Color: posting.GetColor(),
			})
		}
		if temporal := pattern.GetTemporal(); temporal != nil {
			item.Temporal = &temporalStatsJSON{TransactionsPerDay: temporal.GetTransactionsPerDay()}
			if temporal.GetFirstSeen() != nil {
				item.Temporal.FirstSeen = temporal.GetFirstSeen().AsTime().Format(time.RFC3339)
			}
			if temporal.GetLastSeen() != nil {
				item.Temporal.LastSeen = temporal.GetLastSeen().AsTime().Format(time.RFC3339)
			}
			for _, bucket := range temporal.GetPeakHours() {
				item.Temporal.PeakHours = append(item.Temporal.PeakHours, &hourBucketJSON{Hour: bucket.GetHour(), Count: bucket.GetCount()})
			}
		}
		for _, volume := range pattern.GetVolumeStats() {
			item.VolumeStats = append(item.VolumeStats, &assetVolumeStatsJSON{
				Asset: volume.GetAsset(), TotalVolume: volume.GetTotalVolume(), AverageVolume: volume.GetAverageVolume(),
				MinVolume: volume.GetMinVolume(), MaxVolume: volume.GetMaxVolume(), TransactionCount: volume.GetTransactionCount(),
			})
		}
		result.FlowPatterns = append(result.FlowPatterns, item)
	}
	return emitJSON(host, operationID, sdk.ResultObject, result, nil)
}

func postingStructureName(value servicepb.PostingStructure) string {
	switch value {
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

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
