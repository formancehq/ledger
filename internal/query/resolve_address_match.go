package query

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// resolvedAddressMatch describes the predicate without choosing an iterator
// direction. Transaction index readiness precedes parameter resolution.
type resolvedAddressMatch struct {
	value  string
	prefix bool
	role   commonpb.AddressRole
}

func resolveAddressMatch(ctx *compileCtx, am *commonpb.AddressMatch) (resolvedAddressMatch, error) {
	match := resolvedAddressMatch{role: am.GetRole()}
	if ctx.target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		id, label := txAddressIndexID(match.role)
		if _, err := requireIndexReady(ctx, id, label); err != nil {
			return resolvedAddressMatch{}, err
		}
	}

	var err error
	switch m := am.GetMatch().(type) {
	case *commonpb.AddressMatch_HardcodedPrefix:
		match.value, match.prefix = m.HardcodedPrefix, true
	case *commonpb.AddressMatch_HardcodedExact:
		match.value = m.HardcodedExact
	case *commonpb.AddressMatch_ParamPrefix:
		match.value, err = extractString(ctx.params, m.ParamPrefix)
		match.prefix = true
	case *commonpb.AddressMatch_ParamExact:
		match.value, err = extractString(ctx.params, m.ParamExact)
	default:
		return resolvedAddressMatch{}, domain.NewFilterCompilationError("unknown address match type: %T", am.GetMatch())
	}

	return match, err
}
