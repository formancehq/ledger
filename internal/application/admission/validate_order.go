package admission

import (
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/formancehq/ledger/v3/internal/adapter/v2/celrewrite"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// validateOrder validates storage-safety invariants on a fully-constructed order
// before it enters the Raft pipeline. This is the single validation gate for all
// write paths (gRPC, HTTP, bulk).
func validateOrder(order *raftcmdpb.Order) error {
	if err := validateOrderLedgerName(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderMetadata(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderAccountAddresses(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderContent(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderPreparedQuery(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderMirrorSource(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	return nil
}

// validateOrderLedgerName validates the ledger name carried by the LedgerScopedOrder
// wrapper. System-scoped orders have no ledger to validate.
func validateOrderLedgerName(order *raftcmdpb.Order) domain.Describable {
	ls := order.GetLedgerScoped()
	if ls == nil {
		return nil
	}

	if err := domain.ValidateLedgerName(ls.GetLedger()); err != nil {
		return err
	}

	// Reserve the ledger name "_" for the system / non-ledger HTTP routes, which
	// all live under /v3/_/… so they never shadow a real ledger (see
	// ErrLedgerNameReservedPrefix and internal/adapter/http/handler.go). Applied
	// to every ledger-scoped order, not just CreateLedger: a "_" ledger can
	// never legitimately exist, so rejecting it everywhere is safe and keeps the
	// rule in one place.
	if ls.GetLedger() == "_" {
		return ErrLedgerNameReservedPrefix
	}

	return nil
}

// validateOrderMetadata validates that all metadata keys and values in the order
// are safe for Pebble key encoding.
func validateOrderMetadata(order *raftcmdpb.Order) domain.Describable {
	ls := order.GetLedgerScoped()
	if ls == nil {
		return nil
	}

	switch p := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_Apply:
		return validateApplyMetadata(p.Apply)
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		return validateMetadataMap(p.SaveLedgerMetadata.GetMetadata())
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		return domain.ValidateMetadataKey(p.DeleteLedgerMetadata.GetKey())
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		return validateMirrorMetadata(p.MirrorIngest.GetEntry())
	default:
		return nil
	}
}

// validateApplyMetadata validates metadata within a LedgerApplyOrder.
func validateApplyMetadata(apply *raftcmdpb.LedgerApplyOrder) domain.Describable {
	switch d := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		if err := validateMetadataMap(d.CreateTransaction.GetMetadata()); err != nil {
			return err
		}

		for account, mm := range d.CreateTransaction.GetAccountMetadata() {
			if mm != nil {
				if err := validateMetadataMap(mm.GetValues()); err != nil {
					return &domain.ErrAccountValidation{Account: account, Cause: err}
				}
			}
		}

		return nil
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		// processRevertTransaction stores order.GetMetadata() straight into
		// the revert log payload, so the metadata-key invariants (non-empty,
		// no NUL bytes) must be checked here too. Without this gate a
		// client-supplied empty or NUL-bearing key reaches the canonical
		// Pebble key layout via the revert log and corrupts read-index
		// entries (#322).
		return validateMetadataMap(d.RevertTransaction.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		return validateMetadataMap(d.AddMetadata.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		return domain.ValidateMetadataKey(d.DeleteMetadata.GetKey())
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		return domain.ValidateMetadataKey(d.SetMetadataFieldType.GetKey())
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		return domain.ValidateMetadataKey(d.RemoveMetadataFieldType.GetKey())
	default:
		return nil
	}
}

// validateMirrorMetadata validates metadata supplied by mirror ingest orders.
func validateMirrorMetadata(entry *raftcmdpb.MirrorLogEntry) domain.Describable {
	switch d := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		if err := validateMetadataMap(d.CreatedTransaction.GetMetadata()); err != nil {
			return err
		}

		for account, mm := range d.CreatedTransaction.GetAccountMetadata() {
			if mm != nil {
				if err := validateMetadataMap(mm.GetValues()); err != nil {
					return &domain.ErrAccountValidation{Account: account, Cause: err}
				}
			}
		}

		return nil
	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		return validateMetadataMap(d.SavedMetadata.GetMetadata())
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		return validateMetadataMap(d.RevertedTransaction.GetMetadata())
	default:
		return nil
	}
}

// validateOrderAccountAddresses validates account addresses in non-transaction orders
// (metadata targets). Transaction postings are validated in the processor after
// Numscript resolution.
func validateOrderAccountAddresses(order *raftcmdpb.Order) domain.Describable {
	apply, ok := order.GetLedgerScoped().GetPayload().(*raftcmdpb.LedgerScopedOrder_Apply)
	if !ok {
		return nil
	}

	switch d := apply.Apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		if t := d.AddMetadata.GetTarget().GetAccount(); t != nil {
			return domain.ValidateAccountAddress(t.GetAddr())
		}

		return nil
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		if t := d.DeleteMetadata.GetTarget().GetAccount(); t != nil {
			return domain.ValidateAccountAddress(t.GetAddr())
		}

		return nil
	default:
		return nil
	}
}

// validateOrderContent enforces structural well-formedness on the order
// payload (currently CreateTransaction). It rejects orders that declare no
// content source (no postings, no inline script, no script reference) and
// orders that combine explicit postings with a script — both shapes silently
// produce an unintended transaction at the FSM (#452).
//
// The "result must contain ≥1 posting" invariant — needed to catch numscripts
// that execute fine but emit no postings — lives on the FSM
// (processCreateTransaction) because only it sees the post-producer result.
func validateOrderContent(order *raftcmdpb.Order) domain.Describable {
	apply, ok := order.GetLedgerScoped().GetPayload().(*raftcmdpb.LedgerScopedOrder_Apply)
	if !ok {
		return nil
	}

	ct, ok := apply.Apply.GetData().(*raftcmdpb.LedgerApplyOrder_CreateTransaction)
	if !ok {
		return nil
	}

	o := ct.CreateTransaction
	hasPostings := len(o.GetPostings()) > 0
	hasInlineScript := o.GetScript() != nil && o.GetScript().GetPlain() != ""
	// Two booleans on the reference because the two gates have different needs:
	//   - refPresent drives the conflict check: any reference (even nameless)
	//     signals the caller intended the script path, so postings alongside is
	//     ambiguous and must be rejected.
	//   - refValid drives the empty-payload check: a nameless reference is not
	//     real content (the FSM would surface ErrNumscriptNotFound{Name:""}),
	//     so an order with only `scriptReference: {}` is empty.
	refPresent := o.GetNumscriptReference() != nil
	refValid := refPresent && o.GetNumscriptReference().GetName() != ""

	switch {
	case hasPostings && (hasInlineScript || refPresent):
		return domain.ErrPostingsAndScriptConflict
	case !hasPostings && !hasInlineScript && !refValid:
		return domain.ErrEmptyTransaction
	}

	return nil
}

// validateOrderPreparedQuery rejects prepared-query orders whose payload is
// malformed. After moving `ledger` off `common.PreparedQuery` onto the
// surrounding wrapper (PR #522), a request with a valid wrapper ledger but
// a nil/empty `query` (Create) or empty `name` (Update/Delete) no longer
// fails at `loadLedger("")`; it would silently reach the FSM and store /
// look up a nameless entry. This gate plus the matching FSM-side guard in
// processor_prepared_query.go closes the regression flagged on #522.
func validateOrderPreparedQuery(order *raftcmdpb.Order) domain.Describable {
	switch p := order.GetLedgerScoped().GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_CreatePreparedQuery:
		q := p.CreatePreparedQuery.GetQuery()
		if q == nil {
			return domain.ErrPreparedQueryRequired
		}

		return domain.ValidatePreparedQueryName(q.GetName())
	case *raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery:
		return domain.ValidatePreparedQueryName(p.UpdatePreparedQuery.GetName())
	case *raftcmdpb.LedgerScopedOrder_DeletePreparedQuery:
		return domain.ValidatePreparedQueryName(p.DeletePreparedQuery.GetName())
	default:
		return nil
	}
}

// validateOrderMirrorSource enforces structural well-formedness on the
// optional MirrorSource carried by a CreateLedger order. Mirror ledgers go
// through the standard create path, so a missing/blank field reaches the
// FSM and is only surfaced when the mirror worker actually tries to open a
// connection -- well after the ledger has been persisted via Raft. This
// gate fails fast at admission so a malformed mirror config is rejected
// before it lands in the audit chain.
func validateOrderMirrorSource(order *raftcmdpb.Order) domain.Describable {
	// System-scoped orders carry no mirror source. The proto-generated
	// GetLedgerScoped/GetPayload getters are nil-safe by themselves, but
	// the explicit guard mirrors the rest of the validators in this file.
	ls := order.GetLedgerScoped()
	if ls == nil {
		return nil
	}

	create, ok := ls.GetPayload().(*raftcmdpb.LedgerScopedOrder_CreateLedger)
	if !ok {
		return nil
	}

	src := create.CreateLedger.GetMirrorSource()
	if src == nil {
		return nil
	}

	// Compile the CEL rewrite rules before the order reaches the audit chain, so
	// a malformed rule (bad syntax, wrong output type, over the static caps)
	// fails fast instead of stalling the mirror worker on every batch. NewRewriter
	// performs exactly the same compilation the worker will, so a nil error here
	// guarantees the worker can build the rewriter.
	if _, err := celrewrite.NewRewriter(src.GetRewriteRules()); err != nil {
		return ErrMirrorRewriteRuleInvalid
	}

	pg := src.GetPostgres()
	if pg == nil {
		return nil
	}

	iam := pg.GetAwsIamAuth()
	if iam == nil {
		return nil
	}

	if iam.GetRegion() == "" {
		return ErrMirrorIAMRegionRequired
	}

	// The SigV4 token written to ConnConfig.Password is a short-lived bearer
	// credential; admitting a mirror with a non-TLS sslmode would let it
	// travel in cleartext. Use pgx's own parser to avoid string-level
	// bypasses (e.g. quoted keyword=value DSN fragments). The same guard
	// runs at the runtime layer for direct gRPC callers; this admission
	// gate fails earlier, before the order touches the audit chain.
	if !dsnEnforcesTLS(pg.GetDsn()) {
		return ErrMirrorIAMRequiresTLS
	}

	return nil
}

// dsnEnforcesTLS reports whether the DSN meets the admission gate for IAM
// auth: URI form, explicit sslmode in the raw string, and TLS on every
// pgx connect attempt. Mirrors the runtime gate in internal/adapter/v2 —
// see the comment on buildPgxPoolConfig for the full rationale.
//
// Duplicated in-file to keep admission independent of the v2 adapter
// package. On parse error, returns false; the runtime gate surfaces a more
// precise error when the mirror worker actually starts.
func dsnEnforcesTLS(dsn string) bool {
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		return false
	}

	u, err := url.Parse(dsn)
	if err != nil || !u.Query().Has("sslmode") {
		return false
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return false
	}

	if cfg.ConnConfig.TLSConfig == nil {
		return false
	}

	for _, fb := range cfg.ConnConfig.Fallbacks {
		if fb == nil || fb.TLSConfig == nil {
			return false
		}
	}

	return true
}

// validateMetadataMap validates all keys and values in a metadata map.
// Value-level failures are wrapped in ErrMetadataKeyValidation so the
// offending key reaches operator logs and the gRPC ErrorInfo metadata
// (rather than being dropped, which the first pass of this refactor did
// before paul-nicolas's review).
func validateMetadataMap(m map[string]*commonpb.MetadataValue) domain.Describable {
	for key, value := range m {
		if err := domain.ValidateMetadataKey(key); err != nil {
			return err
		}

		if err := domain.ValidateMetadataValue(value); err != nil {
			return &domain.ErrMetadataKeyValidation{Key: key, Cause: err}
		}
	}

	return nil
}
