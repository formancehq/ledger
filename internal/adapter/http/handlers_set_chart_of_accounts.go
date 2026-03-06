package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleSetChartOfAccounts handles PUT /{ledgerName}/chart-of-accounts.
func (s *Server) handleSetChartOfAccounts(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))

		return
	}

	var body chartJSON
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	chart := fromChartJSON(&body)

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_SetChartOfAccounts{
			SetChartOfAccounts: &servicepb.SetChartOfAccountsLedgerRequest{
				Ledger:          ledgerName,
				ChartOfAccounts: chart,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func fromChartJSON(c *chartJSON) *commonpb.ChartOfAccounts {
	if c == nil {
		return nil
	}

	roots := make(map[string]*commonpb.ChartSegment, len(c.Roots))
	for name, segment := range c.Roots {
		roots[name] = fromSegmentJSON(segment)
	}

	return &commonpb.ChartOfAccounts{Roots: roots}
}

func fromSegmentJSON(s *chartSegmentJSON) *commonpb.ChartSegment {
	if s == nil {
		return nil
	}

	result := &commonpb.ChartSegment{
		Account: s.Account,
	}
	if len(s.Children) > 0 {
		result.Children = make(map[string]*commonpb.ChartSegment, len(s.Children))
		for name, child := range s.Children {
			result.Children[name] = fromSegmentJSON(child)
		}
	}

	if s.Variable != nil {
		result.Variable = fromVariableJSON(s.Variable)
	}

	return result
}

func fromVariableJSON(v *chartVariableJSON) *commonpb.ChartVariable {
	if v == nil {
		return nil
	}

	result := &commonpb.ChartVariable{
		Name:    v.Name,
		Pattern: v.Pattern,
		Account: v.Account,
	}
	if len(v.Children) > 0 {
		result.Children = make(map[string]*commonpb.ChartSegment, len(v.Children))
		for name, child := range v.Children {
			result.Children[name] = fromSegmentJSON(child)
		}
	}

	if v.Variable != nil {
		result.Variable = fromVariableJSON(v.Variable)
	}

	return result
}
