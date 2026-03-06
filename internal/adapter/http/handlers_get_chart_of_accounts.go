package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// chartOfAccountsJSON is the camelCase JSON DTO for chart of accounts response.
type chartOfAccountsJSON struct {
	ChartOfAccounts *chartJSON `json:"chartOfAccounts,omitempty"`
	EnforcementMode string     `json:"enforcementMode"`
}

type chartJSON struct {
	Roots map[string]*chartSegmentJSON `json:"roots,omitempty"`
}

type chartSegmentJSON struct {
	Account  bool                         `json:"account,omitempty"`
	Children map[string]*chartSegmentJSON `json:"children,omitempty"`
	Variable *chartVariableJSON           `json:"variable,omitempty"`
}

type chartVariableJSON struct {
	Name     string                       `json:"name"`
	Pattern  string                       `json:"pattern,omitempty"`
	Account  bool                         `json:"account,omitempty"`
	Children map[string]*chartSegmentJSON `json:"children,omitempty"`
	Variable *chartVariableJSON           `json:"variable,omitempty"`
}

func toChartJSON(chart *commonpb.ChartOfAccounts) *chartJSON {
	if chart == nil {
		return nil
	}

	roots := make(map[string]*chartSegmentJSON, len(chart.GetRoots()))
	for name, segment := range chart.GetRoots() {
		roots[name] = toSegmentJSON(segment)
	}

	return &chartJSON{Roots: roots}
}

func toSegmentJSON(segment *commonpb.ChartSegment) *chartSegmentJSON {
	if segment == nil {
		return nil
	}

	result := &chartSegmentJSON{
		Account: segment.GetAccount(),
	}
	if len(segment.GetChildren()) > 0 {
		result.Children = make(map[string]*chartSegmentJSON, len(segment.GetChildren()))
		for name, child := range segment.GetChildren() {
			result.Children[name] = toSegmentJSON(child)
		}
	}

	if segment.GetVariable() != nil {
		result.Variable = toVariableJSON(segment.GetVariable())
	}

	return result
}

func toVariableJSON(v *commonpb.ChartVariable) *chartVariableJSON {
	if v == nil {
		return nil
	}

	result := &chartVariableJSON{
		Name:    v.GetName(),
		Pattern: v.GetPattern(),
		Account: v.GetAccount(),
	}
	if len(v.GetChildren()) > 0 {
		result.Children = make(map[string]*chartSegmentJSON, len(v.GetChildren()))
		for name, child := range v.GetChildren() {
			result.Children[name] = toSegmentJSON(child)
		}
	}

	if v.GetVariable() != nil {
		result.Variable = toVariableJSON(v.GetVariable())
	}

	return result
}

func enforcementModeToString(mode commonpb.ChartEnforcementMode) string {
	switch mode {
	case commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT:
		return "AUDIT"
	default:
		return "STRICT"
	}
}

func parseEnforcementMode(s string) (commonpb.ChartEnforcementMode, error) {
	switch s {
	case "STRICT", "strict":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, nil
	case "AUDIT", "audit":
		return commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, nil
	default:
		return 0, errors.New("invalid enforcement mode: must be STRICT or AUDIT")
	}
}

// handleGetChartOfAccounts handles GET /{ledgerName}/chart-of-accounts.
func (s *Server) handleGetChartOfAccounts(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))

		return
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, &chartOfAccountsJSON{
		ChartOfAccounts: toChartJSON(ledgerInfo.GetChartOfAccounts()),
		EnforcementMode: enforcementModeToString(ledgerInfo.GetEnforcementMode()),
	})
}
