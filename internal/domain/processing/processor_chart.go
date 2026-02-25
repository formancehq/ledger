package processing

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// collectViolations checks a list of addresses and returns violations for those
// not matching the chart. In STRICT mode, returns an error on the first invalid address.
// In AUDIT mode, collects all violations and returns them.
func collectViolations(
	addresses []string,
	chart *commonpb.ChartOfAccounts,
	mode commonpb.ChartEnforcementMode,
) ([]*commonpb.ChartViolation, error) {
	var violations []*commonpb.ChartViolation
	seen := make(map[string]struct{})
	for _, address := range addresses {
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		if !validateAccountInChart(address, chart) {
			if mode == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
				return nil, &domain.ErrAccountNotInChart{Address: address}
			}
			violations = append(violations, &commonpb.ChartViolation{Address: address})
		}
	}
	return violations, nil
}

// segmentNameRegexp validates chart segment names (same as account segment format).
var segmentNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// validateChart validates the chart of accounts structure itself.
func validateChart(chart *commonpb.ChartOfAccounts) error {
	if chart == nil || len(chart.Roots) == 0 {
		return fmt.Errorf("chart must have at least one root segment")
	}

	hasAccount := false
	for name, segment := range chart.Roots {
		if !segmentNameRegexp.MatchString(name) {
			return fmt.Errorf("invalid segment name: %q", name)
		}
		found, err := validateSegment(segment)
		if err != nil {
			return err
		}
		if found {
			hasAccount = true
		}
	}

	if !hasAccount {
		return fmt.Errorf("chart must have at least one node with account: true")
	}

	return nil
}

// validateSegment recursively validates a chart segment and returns whether
// any node in the subtree has account=true.
func validateSegment(segment *commonpb.ChartSegment) (bool, error) {
	hasAccount := segment.Account

	for name, child := range segment.Children {
		if !segmentNameRegexp.MatchString(name) {
			return false, fmt.Errorf("invalid segment name: %q", name)
		}
		found, err := validateSegment(child)
		if err != nil {
			return false, err
		}
		if found {
			hasAccount = true
		}
	}

	if segment.Variable != nil {
		found, err := validateVariable(segment.Variable)
		if err != nil {
			return false, err
		}
		if found {
			hasAccount = true
		}
	}

	return hasAccount, nil
}

// validateVariable recursively validates a chart variable.
func validateVariable(v *commonpb.ChartVariable) (bool, error) {
	if v.Name == "" {
		return false, fmt.Errorf("variable name must be non-empty")
	}

	if v.Pattern != "" {
		if _, err := regexp.Compile(v.Pattern); err != nil {
			return false, fmt.Errorf("invalid pattern %q: %w", v.Pattern, err)
		}
	}

	hasAccount := v.Account

	for name, child := range v.Children {
		if !segmentNameRegexp.MatchString(name) {
			return false, fmt.Errorf("invalid segment name: %q", name)
		}
		found, err := validateSegment(child)
		if err != nil {
			return false, err
		}
		if found {
			hasAccount = true
		}
	}

	if v.Variable != nil {
		found, err := validateVariable(v.Variable)
		if err != nil {
			return false, err
		}
		if found {
			hasAccount = true
		}
	}

	return hasAccount, nil
}

// validateAccountInChart checks whether a single account address is valid
// according to the chart of accounts. The "world" account is always valid.
func validateAccountInChart(address string, chart *commonpb.ChartOfAccounts) bool {
	if address == "world" {
		return true
	}

	segments := strings.Split(address, ":")
	if len(segments) == 0 {
		return false
	}

	// Look up the first segment in roots
	root, ok := chart.Roots[segments[0]]
	if !ok {
		return false
	}

	return walkSegment(segments[1:], root)
}

// walkSegment walks the chart tree matching address segments.
func walkSegment(remaining []string, segment *commonpb.ChartSegment) bool {
	if len(remaining) == 0 {
		return segment.Account
	}

	next := remaining[0]

	// Fixed children first
	if child, ok := segment.Children[next]; ok {
		return walkSegment(remaining[1:], child)
	}

	// Variable child
	if segment.Variable != nil {
		return walkVariable(remaining, segment.Variable)
	}

	return false
}

// walkVariable attempts to match the current segment against a variable node.
func walkVariable(remaining []string, v *commonpb.ChartVariable) bool {
	if len(remaining) == 0 {
		return false
	}

	current := remaining[0]

	// Check pattern if declared
	if v.Pattern != "" {
		matched, err := regexp.MatchString(v.Pattern, current)
		if err != nil || !matched {
			return false
		}
	}

	// Consumed the current segment via the variable
	rest := remaining[1:]
	if len(rest) == 0 {
		return v.Account
	}

	// Continue with fixed children first
	next := rest[0]
	if child, ok := v.Children[next]; ok {
		return walkSegment(rest[1:], child)
	}

	// Then variable child
	if v.Variable != nil {
		return walkVariable(rest, v.Variable)
	}

	return false
}

// validatePostingsInChart validates all postings against the chart of accounts.
// In STRICT mode, returns an error on the first invalid address.
// In AUDIT mode, collects violations and returns them as warnings.
// If chart is nil, no validation is performed.
func validatePostingsInChart(
	postings []*commonpb.Posting,
	chart *commonpb.ChartOfAccounts,
	mode commonpb.ChartEnforcementMode,
) ([]*commonpb.ChartViolation, error) {
	if chart == nil {
		return nil, nil
	}

	addresses := make([]string, 0, len(postings)*2)
	for _, posting := range postings {
		addresses = append(addresses, posting.Source, posting.Destination)
	}
	return collectViolations(addresses, chart, mode)
}

// validateAccountInChartForAudit validates a single account address against the chart.
// In STRICT mode, returns an error if invalid.
// In AUDIT mode, returns a violation as warning.
// If chart is nil, no validation is performed.
func validateAccountInChartForAudit(
	address string,
	chart *commonpb.ChartOfAccounts,
	mode commonpb.ChartEnforcementMode,
) ([]*commonpb.ChartViolation, error) {
	if chart == nil {
		return nil, nil
	}
	return collectViolations([]string{address}, chart, mode)
}

// processSetChartOfAccounts sets the chart of accounts on a ledger.
func (p *RequestProcessor) processSetChartOfAccounts(
	ledgerName string,
	order *raftcmdpb.SetChartOfAccountsOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	if err := validateChart(order.ChartOfAccounts); err != nil {
		return nil, &domain.ErrInvalidChart{Details: err.Error()}
	}

	info.ChartOfAccounts = order.ChartOfAccounts
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SetChartOfAccounts{
			SetChartOfAccounts: &commonpb.SetChartOfAccountsLog{
				ChartOfAccounts: order.ChartOfAccounts,
			},
		},
	}, nil
}

// processSetChartEnforcementMode sets the enforcement mode for chart validation.
func (p *RequestProcessor) processSetChartEnforcementMode(
	ledgerName string,
	order *raftcmdpb.SetChartEnforcementModeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	info.EnforcementMode = order.EnforcementMode
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SetChartEnforcementMode{
			SetChartEnforcementMode: &commonpb.SetChartEnforcementModeLog{
				EnforcementMode: order.EnforcementMode,
			},
		},
	}, nil
}
