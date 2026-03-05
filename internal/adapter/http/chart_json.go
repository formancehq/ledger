package http

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// Chart of Accounts JSON DTOs — still used by the analyze and create-ledger handlers.

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
