package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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
		Assets:       p.GetAssets(),
		MetadataKeys: p.GetMetadataKeys(),
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
		Examples:        s.GetExamples(),
	}
}

// handleAnalyzeAccounts handles GET /{ledgerName}/analyze-accounts.
func (s *Server) handleAnalyzeAccounts(w http.ResponseWriter, r *http.Request) {
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

	resp, err := s.backend.AnalyzeAccounts(r.Context(), ledgerName, variableThreshold, nil)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, toAnalyzeAccountsJSON(resp))
}
