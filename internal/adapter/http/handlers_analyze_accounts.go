package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// analyzeAccountsResponseJSON is the camelCase JSON DTO for AnalyzeAccountsResponse.
type analyzeAccountsResponseJSON struct {
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

// nonNilStrings returns s unchanged when non-nil, or an empty (non-nil) slice
// otherwise. The OpenAPI schema types these fields as plain (non-nullable)
// arrays; a proto GetX() getter returns nil for an absent repeated field,
// which would otherwise serialize as JSON null and violate the schema. Shared
// with handlers_analyze_transactions.go, whose FlowPattern.MetadataKeys has
// the same shape.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

func toAnalyzeAccountsJSON(resp *servicepb.AnalyzeAccountsResponse) *analyzeAccountsResponseJSON {
	result := &analyzeAccountsResponseJSON{
		TotalAccounts: resp.GetTotalAccounts(),
	}

	result.Patterns = make([]*accountPatternJSON, 0, len(resp.GetPatterns()))
	for _, p := range resp.GetPatterns() {
		result.Patterns = append(result.Patterns, toAccountPatternJSON(p))
	}

	return result
}

func toAccountPatternJSON(p *servicepb.AccountPattern) *accountPatternJSON {
	result := &accountPatternJSON{
		Pattern:      p.GetPattern(),
		AccountCount: p.GetAccountCount(),
		Assets:       nonNilStrings(p.GetAssets()),
		MetadataKeys: nonNilStrings(p.GetMetadataKeys()),
	}

	result.Segments = make([]*patternSegmentJSON, 0, len(p.GetSegments()))
	for _, s := range p.GetSegments() {
		result.Segments = append(result.Segments, toPatternSegmentJSON(s))
	}

	return result
}

func toPatternSegmentJSON(s *servicepb.PatternSegment) *patternSegmentJSON {
	segType := "fixed"
	if s.GetType() == servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE {
		segType = "variable"
	}

	return &patternSegmentJSON{
		Position:        s.GetPosition(),
		Type:            segType,
		FixedValue:      s.GetFixedValue(),
		VariableName:    s.GetVariableName(),
		InferredPattern: s.GetInferredPattern(),
		UniqueValues:    s.GetUniqueValues(),
		Examples:        nonNilStrings(s.GetExamples()),
	}
}

// handleAnalyzeAccounts handles GET /{ledgerName}/analyze-accounts.
func (s *Server) handleAnalyzeAccounts(w http.ResponseWriter, r *http.Request) {
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

	resp, err := s.backend.AnalyzeAccounts(r.Context(), ledgerName, variableThreshold, nil)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, toAnalyzeAccountsJSON(resp))
}
