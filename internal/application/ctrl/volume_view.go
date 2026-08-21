package ctrl

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

// HistoricalBalanceSelector is application-layer read selection. It deliberately
// stays outside query.AggregateOptions, whose fields describe only monetary
// aggregation and are also used by prepared queries and FSM sentinels.
type HistoricalBalanceSelector struct {
	At          uint64
	Temporality balancehistorystore.Temporality
}

// AggregateVolumesReadOptions controls consistency/view selection separately
// from AggregateOptions.
type AggregateVolumesReadOptions struct {
	HistoricalBalance *HistoricalBalanceSelector
	MinLogSequence    uint64
}

// HistoricalBalanceViewToken identifies the immutable history manifest used by a result.
type HistoricalBalanceViewToken struct {
	RequestedAt     uint64
	Temporality     balancehistorystore.Temporality
	Ledger          string
	AuditWatermark  uint64
	LogWatermark    uint64
	ManifestVersion uint64
	Token           string
}

// AggregateVolumesResult preserves the existing monetary response while
// carrying historical-balance provenance out-of-band at the transport layer.
type AggregateVolumesResult struct {
	Aggregate *commonpb.AggregateResult
	View      *HistoricalBalanceViewToken
}

// VolumeViewProvider hides manifest pinning, lag waiting, temporal store
// layout, and token construction from controllers and transports.
type VolumeViewProvider interface {
	Open(
		ctx context.Context,
		ledgerName string,
		selector HistoricalBalanceSelector,
		minLogSequence uint64,
	) (*HistoricalVolumeView, error)
	Status(ctx context.Context, ledgerName string) (*servicepb.GetHistoricalBalancesStatusResponse, error)
}

// LocalVolumeViewProvider opens views from this replica's peer store.
type LocalVolumeViewProvider struct {
	store *balancehistorystore.Store
}

func NewLocalVolumeViewProvider(store *balancehistorystore.Store) *LocalVolumeViewProvider {
	return &LocalVolumeViewProvider{store: store}
}

func (p *LocalVolumeViewProvider) Status(_ context.Context, ledgerName string) (*servicepb.GetHistoricalBalancesStatusResponse, error) {
	response := &servicepb.GetHistoricalBalancesStatusResponse{Ledger: ledgerName}
	if p == nil || p.store == nil {
		response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR
		response.Error = "historical-balance peer store is unavailable"

		return response, nil
	}

	manifest, err := p.store.Manifest()
	if err != nil {
		response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR
		response.Error = err.Error()

		return response, nil
	}
	response.AuditWatermark = manifest.AuditWatermark
	response.LogWatermark = manifest.LogWatermark
	if !sort.StringsAreSorted(manifest.Ledgers) {
		response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR
		response.Error = "configured ledger names are not sorted"

		return response, nil
	}
	index := sort.SearchStrings(manifest.Ledgers, ledgerName)
	if index == len(manifest.Ledgers) || manifest.Ledgers[index] != ledgerName {
		response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_DISABLED

		return response, nil
	}
	if err := p.store.ReadinessError(); err != nil {
		var building *balancehistorystore.ErrBuilding
		if errors.As(err, &building) {
			response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_BUILDING

			return response, nil
		}
		response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR
		response.Error = err.Error()

		return response, nil
	}
	view, err := p.store.OpenView(manifest.LogWatermark)
	if err != nil {
		var building *balancehistorystore.ErrBuilding
		if errors.As(err, &building) {
			response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_BUILDING

			return response, nil
		}
		response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR
		response.Error = err.Error()

		return response, nil
	}
	if err := view.Close(); err != nil {
		response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR
		response.Error = err.Error()

		return response, nil
	}
	response.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_READY

	return response, nil
}

func (p *LocalVolumeViewProvider) Open(
	ctx context.Context,
	ledgerName string,
	selector HistoricalBalanceSelector,
	minLogSequence uint64,
) (*HistoricalVolumeView, error) {
	if p == nil || p.store == nil {
		return nil, &balancehistorystore.ErrSourceMissing{Detail: "balance history projection is not configured"}
	}
	if selector.Temporality != balancehistorystore.TemporalityEffective && selector.Temporality != balancehistorystore.TemporalityInsertion {
		return nil, fmt.Errorf("invalid historical-balance temporality %d", selector.Temporality)
	}
	if minLogSequence > 0 {
		if err := p.store.WaitForLogWatermark(ctx, minLogSequence); err != nil {
			return nil, err
		}
	}

	view, err := p.store.OpenViewContext(ctx, minLogSequence)
	if err != nil {
		return nil, err
	}
	manifest := view.Manifest()
	if !sort.StringsAreSorted(manifest.Ledgers) {
		_ = view.Close()

		return nil, &balancehistorystore.ErrCorrupt{Detail: "configured ledger names are not sorted"}
	}
	index := sort.SearchStrings(manifest.Ledgers, ledgerName)
	if index == len(manifest.Ledgers) || manifest.Ledgers[index] != ledgerName {
		_ = view.Close()

		return nil, &balancehistorystore.ErrSourceMissing{Detail: fmt.Sprintf("historical balances are disabled for ledger %q", ledgerName)}
	}

	return &HistoricalVolumeView{
		view:       view,
		ledgerName: ledgerName,
		selector:   selector,
		token: HistoricalBalanceViewToken{
			RequestedAt:     selector.At,
			Temporality:     selector.Temporality,
			Ledger:          ledgerName,
			AuditWatermark:  manifest.AuditWatermark,
			LogWatermark:    manifest.LogWatermark,
			ManifestVersion: manifest.Version,
			Token:           encodeVolumeViewToken(ledgerName, selector, manifest),
		},
	}, nil
}

// HistoricalVolumeView is immutable for its lifetime.
type HistoricalVolumeView struct {
	view       *balancehistorystore.View
	ledgerName string
	selector   HistoricalBalanceSelector
	token      HistoricalBalanceViewToken
}

func (v *HistoricalVolumeView) Aggregate(ctx context.Context, accounts []string, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
	return v.aggregate(ctx, accounts, nil, nil, opts)
}

func (v *HistoricalVolumeView) aggregate(
	ctx context.Context,
	accounts []string,
	accountPrefixes []string,
	match func(account string) bool,
	opts query.AggregateOptions,
) (*commonpb.AggregateResult, error) {
	if v == nil || v.view == nil {
		return nil, errors.New("historical volume view is closed")
	}

	return query.AggregateHistoricalVolumesSelected(ctx, v.view, v.ledgerName, v.selector.Temporality, v.selector.At, accounts, accountPrefixes, match, opts)
}

func (v *HistoricalVolumeView) Token() HistoricalBalanceViewToken {
	return v.token
}

func (v *HistoricalVolumeView) Close() error {
	if v == nil || v.view == nil {
		return nil
	}

	err := v.view.Close()
	v.view = nil

	return err
}

func encodeVolumeViewToken(ledgerName string, selector HistoricalBalanceSelector, manifest balancehistorystore.Manifest) string {
	buffer := make([]byte, 0, len(ledgerName)+1+8*4+len(manifest.AuditHash))
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(ledgerName)))
	buffer = append(buffer, ledgerName...)
	buffer = append(buffer, byte(selector.Temporality))
	buffer = binary.BigEndian.AppendUint64(buffer, selector.At)
	buffer = binary.BigEndian.AppendUint64(buffer, manifest.Version)
	buffer = binary.BigEndian.AppendUint64(buffer, manifest.AuditWatermark)
	buffer = binary.BigEndian.AppendUint64(buffer, manifest.LogWatermark)
	buffer = append(buffer, manifest.AuditHash...)

	// The token is an opaque, reversible identity encoding, not a checksum.
	// Integrity remains the responsibility of the audit hash chain and Pebble.
	return base64.RawURLEncoding.EncodeToString(buffer)
}

type temporalFilterKind uint8

const (
	temporalFilterAddress temporalFilterKind = iota
	temporalFilterCurrent
	temporalFilterMixed
)

type temporalFilterOperator uint8

const (
	temporalFilterLeaf temporalFilterOperator = iota
	temporalFilterAnd
	temporalFilterOr
	temporalFilterNot
	temporalFilterFalse
)

// temporalAccountFilterPlan preserves boolean filter semantics while making
// the temporal boundary explicit: hard-coded address leaves can be evaluated
// against historical account identities, while metadata and has-asset
// subtrees are materialized from one current read-store snapshot.
type temporalAccountFilterPlan struct {
	kind            temporalFilterKind
	operator        temporalFilterOperator
	address         *commonpb.AddressMatch
	currentFilter   *commonpb.QueryFilter
	currentAccounts []string
	currentSet      map[string]struct{}
	children        []*temporalAccountFilterPlan
}

type temporalAccountSelection struct {
	accounts        []string
	accountPrefixes []string
	match           func(account string) bool
}

func prepareTemporalAccountSelection(
	ctx context.Context,
	filter *commonpb.QueryFilter,
	compileCurrent func(context.Context, *commonpb.QueryFilter) ([]string, error),
) (temporalAccountSelection, error) {
	plan, err := buildTemporalAccountFilterPlan(filter, 0)
	if err != nil {
		return temporalAccountSelection{}, err
	}
	if err := plan.bindCurrentAccounts(ctx, compileCurrent); err != nil {
		return temporalAccountSelection{}, err
	}

	// A current-only subtree already yields the exact sorted account list, so
	// the history store can seek only those account prefixes.
	if plan.kind == temporalFilterCurrent {
		return temporalAccountSelection{accounts: plan.currentAccounts}, nil
	}

	// Mixed AND trees and exact-address expressions may still have a finite
	// candidate set. Prefix candidates use the history store's prefix-seekable
	// catalog, including mixed OR expressions, without changing the final
	// predicate semantics.
	candidates, bounded := plan.candidates()
	if bounded {
		return temporalAccountSelection{
			accounts:        candidates.accounts,
			accountPrefixes: candidates.prefixes,
			match:           plan.matches,
		}, nil
	}

	return temporalAccountSelection{match: plan.matches}, nil
}

func buildTemporalAccountFilterPlan(filter *commonpb.QueryFilter, depth int) (*temporalAccountFilterPlan, error) {
	if filter == nil {
		return nil, &balancehistorystore.ErrUnsupportedTemporalFilter{Category: "nil-subfilter"}
	}
	if depth >= domain.MaxFilterDepth {
		return nil, domain.ErrFilterTooDeep
	}

	switch node := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_Address:
		if node.Address == nil {
			return nil, &balancehistorystore.ErrUnsupportedTemporalFilter{Category: "invalid-address"}
		}
		switch node.Address.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedExact, *commonpb.AddressMatch_HardcodedPrefix:
			return &temporalAccountFilterPlan{
				kind:     temporalFilterAddress,
				operator: temporalFilterLeaf,
				address:  node.Address,
			}, nil
		case *commonpb.AddressMatch_ParamExact, *commonpb.AddressMatch_ParamPrefix:
			return nil, &balancehistorystore.ErrUnsupportedTemporalFilter{Category: "parameterized-address"}
		default:
			return nil, &balancehistorystore.ErrUnsupportedTemporalFilter{Category: "invalid-address"}
		}

	case *commonpb.QueryFilter_Field, *commonpb.QueryFilter_AccountHasAsset:
		return &temporalAccountFilterPlan{
			kind:          temporalFilterCurrent,
			operator:      temporalFilterLeaf,
			currentFilter: filter,
		}, nil

	case *commonpb.QueryFilter_And:
		return buildTemporalCombinatorPlan(filter, node.And.GetFilters(), temporalFilterAnd, depth)

	case *commonpb.QueryFilter_Or:
		return buildTemporalCombinatorPlan(filter, node.Or.GetFilters(), temporalFilterOr, depth)

	case *commonpb.QueryFilter_Not:
		if node.Not == nil || node.Not.GetFilter() == nil {
			return nil, &balancehistorystore.ErrUnsupportedTemporalFilter{Category: "nil-subfilter"}
		}
		child, err := buildTemporalAccountFilterPlan(node.Not.GetFilter(), depth+1)
		if err != nil {
			return nil, err
		}
		if child.kind == temporalFilterMixed {
			return nil, &balancehistorystore.ErrUnsupportedTemporalFilter{Category: "mixed-not"}
		}
		if child.kind == temporalFilterCurrent {
			// Compile the complete NOT subtree against the current account
			// universe. Evaluating !membership against historical identities
			// would incorrectly include accounts absent from current state.
			return &temporalAccountFilterPlan{
				kind:          temporalFilterCurrent,
				operator:      temporalFilterLeaf,
				currentFilter: filter,
			}, nil
		}

		return &temporalAccountFilterPlan{
			kind:     temporalFilterAddress,
			operator: temporalFilterNot,
			children: []*temporalAccountFilterPlan{child},
		}, nil

	default:
		return nil, &balancehistorystore.ErrUnsupportedTemporalFilter{Category: "account-condition"}
	}
}

func buildTemporalCombinatorPlan(
	filter *commonpb.QueryFilter,
	filters []*commonpb.QueryFilter,
	operator temporalFilterOperator,
	depth int,
) (*temporalAccountFilterPlan, error) {
	if len(filters) == 0 {
		// query.Compile defines empty AND and empty OR as an empty result.
		return &temporalAccountFilterPlan{kind: temporalFilterAddress, operator: temporalFilterFalse}, nil
	}

	children := make([]*temporalAccountFilterPlan, 0, len(filters))
	kind := temporalFilterAddress
	for _, childFilter := range filters {
		child, err := buildTemporalAccountFilterPlan(childFilter, depth+1)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
		if len(children) == 1 {
			kind = child.kind
		} else if kind != child.kind {
			kind = temporalFilterMixed
		}
	}

	if kind == temporalFilterCurrent {
		// Keep current boolean/not universe semantics inside the existing
		// compiler and evaluate the maximal current-only subtree once.
		return &temporalAccountFilterPlan{
			kind:          temporalFilterCurrent,
			operator:      temporalFilterLeaf,
			currentFilter: filter,
		}, nil
	}

	return &temporalAccountFilterPlan{
		kind:     kind,
		operator: operator,
		children: children,
	}, nil
}

func (p *temporalAccountFilterPlan) bindCurrentAccounts(
	ctx context.Context,
	compileCurrent func(context.Context, *commonpb.QueryFilter) ([]string, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.kind == temporalFilterCurrent {
		accounts, err := compileCurrent(ctx, p.currentFilter)
		if err != nil {
			return err
		}
		p.currentAccounts = deduplicateSortedAccounts(accounts)
		p.currentSet = make(map[string]struct{}, len(p.currentAccounts))
		for _, account := range p.currentAccounts {
			p.currentSet[account] = struct{}{}
		}

		return nil
	}

	for _, child := range p.children {
		if err := child.bindCurrentAccounts(ctx, compileCurrent); err != nil {
			return err
		}
	}

	return nil
}

func (p *temporalAccountFilterPlan) matches(account string) bool {
	switch p.operator {
	case temporalFilterLeaf:
		if p.kind == temporalFilterCurrent {
			_, ok := p.currentSet[account]

			return ok
		}
		switch match := p.address.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedExact:
			return account == match.HardcodedExact
		case *commonpb.AddressMatch_HardcodedPrefix:
			return strings.HasPrefix(account, match.HardcodedPrefix)
		default:
			return false
		}
	case temporalFilterAnd:
		for _, child := range p.children {
			if !child.matches(account) {
				return false
			}
		}

		return len(p.children) > 0
	case temporalFilterOr:
		for _, child := range p.children {
			if child.matches(account) {
				return true
			}
		}

		return false
	case temporalFilterNot:
		return len(p.children) == 1 && !p.children[0].matches(account)
	case temporalFilterFalse:
		return false
	default:
		return false
	}
}

type temporalAccountCandidates struct {
	accounts []string
	prefixes []string
}

// candidates returns a bounded union of exact accounts and prefix ranges that
// contains every account the plan can match. false means the predicate needs
// the complete historical universe (for example, NOT(address prefix)).
func (p *temporalAccountFilterPlan) candidates() (temporalAccountCandidates, bool) {
	switch p.operator {
	case temporalFilterLeaf:
		if p.kind == temporalFilterCurrent {
			return temporalAccountCandidates{accounts: append([]string(nil), p.currentAccounts...)}, true
		}
		if exact, ok := p.address.GetMatch().(*commonpb.AddressMatch_HardcodedExact); ok {
			return temporalAccountCandidates{accounts: []string{exact.HardcodedExact}}, true
		}
		if prefix, ok := p.address.GetMatch().(*commonpb.AddressMatch_HardcodedPrefix); ok {
			return temporalAccountCandidates{prefixes: []string{prefix.HardcodedPrefix}}, true
		}

		return temporalAccountCandidates{}, false
	case temporalFilterFalse:
		return temporalAccountCandidates{accounts: []string{}}, true
	case temporalFilterNot:
		return temporalAccountCandidates{}, false
	case temporalFilterAnd:
		var selected temporalAccountCandidates
		selectedBounded := false
		for _, child := range p.children {
			candidate, childBounded := child.candidates()
			if !childBounded {
				continue
			}
			if !selectedBounded {
				selected = candidate
				selectedBounded = true

				continue
			}

			// An exact-only candidate is always preferable to a prefix union:
			// it is finite and can be intersected with another exact set. Any
			// single AND child remains a sound superset; matches applies every
			// remaining condition after the bounded lookup.
			switch {
			case len(selected.prefixes) == 0 && len(candidate.prefixes) == 0:
				selected.accounts = intersectSortedAccounts(selected.accounts, candidate.accounts)
			case len(candidate.prefixes) == 0:
				selected = candidate
			}
		}

		return selected, selectedBounded
	case temporalFilterOr:
		selected := temporalAccountCandidates{
			accounts: make([]string, 0),
			prefixes: make([]string, 0),
		}
		for _, child := range p.children {
			candidate, bounded := child.candidates()
			if !bounded {
				return temporalAccountCandidates{}, false
			}
			selected.accounts = append(selected.accounts, candidate.accounts...)
			selected.prefixes = append(selected.prefixes, candidate.prefixes...)
		}

		selected.accounts = deduplicateSortedAccounts(selected.accounts)
		selected.prefixes = deduplicateSortedAccounts(selected.prefixes)

		return selected, true
	default:
		return temporalAccountCandidates{}, false
	}
}

func deduplicateSortedAccounts(accounts []string) []string {
	if len(accounts) == 0 {
		return []string{}
	}
	accounts = append([]string(nil), accounts...)
	sort.Strings(accounts)
	unique := accounts[:1]
	for _, account := range accounts[1:] {
		if account != unique[len(unique)-1] {
			unique = append(unique, account)
		}
	}

	return unique
}

func intersectSortedAccounts(left, right []string) []string {
	left = deduplicateSortedAccounts(left)
	right = deduplicateSortedAccounts(right)
	intersection := make([]string, 0)
	for i, j := 0, 0; i < len(left) && j < len(right); {
		switch {
		case left[i] < right[j]:
			i++
		case left[i] > right[j]:
			j++
		default:
			intersection = append(intersection, left[i])
			i++
			j++
		}
	}

	return intersection
}
