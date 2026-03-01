package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// analyzeAccountsResponseJSON is the camelCase JSON DTO for AnalyzeAccountsResponse.
type analyzeAccountsResponseJSON struct {
	SuggestedChart *chartOfAccountsJSON `json:"suggestedChart"`
	Patterns       []*accountPatternJSON `json:"patterns"`
	TotalAccounts  uint64               `json:"totalAccounts"`
}

type chartOfAccountsJSON struct {
	Segments []*chartSegmentJSON `json:"segments"`
}

type chartSegmentJSON struct {
	FixedValue string             `json:"fixedValue,omitempty"`
	Variable   *chartVariableJSON `json:"variable,omitempty"`
	Children   []*chartSegmentJSON `json:"children,omitempty"`
}

type chartVariableJSON struct {
	Name            string `json:"name"`
	InferredPattern string `json:"inferredPattern,omitempty"`
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
		TotalAccounts: resp.TotalAccounts,
	}

	if resp.SuggestedChart != nil {
		result.SuggestedChart = toChartOfAccountsJSON(resp.SuggestedChart)
	}

	result.Patterns = make([]*accountPatternJSON, 0, len(resp.Patterns))
	for _, p := range resp.Patterns {
		result.Patterns = append(result.Patterns, toAccountPatternJSON(p))
	}

	return result
}

func toChartOfAccountsJSON(chart *commonpb.ChartOfAccounts) *chartOfAccountsJSON {
	result := &chartOfAccountsJSON{
		Segments: make([]*chartSegmentJSON, 0, len(chart.Segments)),
	}
	for _, s := range chart.Segments {
		result.Segments = append(result.Segments, toChartSegmentJSON(s))
	}
	return result
}

func toChartSegmentJSON(seg *commonpb.ChartSegment) *chartSegmentJSON {
	result := &chartSegmentJSON{
		FixedValue: seg.FixedValue,
	}
	if seg.Variable != nil {
		result.Variable = &chartVariableJSON{
			Name:            seg.Variable.Name,
			InferredPattern: seg.Variable.InferredPattern,
		}
	}
	if len(seg.Children) > 0 {
		result.Children = make([]*chartSegmentJSON, 0, len(seg.Children))
		for _, child := range seg.Children {
			result.Children = append(result.Children, toChartSegmentJSON(child))
		}
	}
	return result
}

func toAccountPatternJSON(p *servicepb.AccountPattern) *accountPatternJSON {
	result := &accountPatternJSON{
		Pattern:      p.Pattern,
		AccountCount: p.AccountCount,
		Assets:       p.Assets,
		MetadataKeys: p.MetadataKeys,
	}
	result.Segments = make([]*patternSegmentJSON, 0, len(p.Segments))
	for _, s := range p.Segments {
		result.Segments = append(result.Segments, toPatternSegmentJSON(s))
	}
	return result
}

func toPatternSegmentJSON(s *servicepb.PatternSegment) *patternSegmentJSON {
	segType := "fixed"
	if s.Type == servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE {
		segType = "variable"
	}
	return &patternSegmentJSON{
		Position:        s.Position,
		Type:            segType,
		FixedValue:      s.FixedValue,
		VariableName:    s.VariableName,
		InferredPattern: s.InferredPattern,
		UniqueValues:    s.UniqueValues,
		Examples:        s.Examples,
	}
}

// handleAnalyzeAccounts handles GET /{ledgerName}/analyze-accounts
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

	resp, err := s.backend.AnalyzeAccounts(r.Context(), ledgerName, variableThreshold)
	if err != nil {
		handleError(w, r, err)
		return
	}

	writeOK(w, toAnalyzeAccountsJSON(resp))
}
