package admission

import (
	"crypto/ed25519"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/formancehq/ledger/v3/internal/adapter/v2/celrewrite"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
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

	if err := validateOrderCreateIndex(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderMirrorSource(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderPostingColors(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderSigningKey(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	return nil
}

// validateOrderPostingColors validates Color on every direct Posting attached
// to a CreateTransaction order, BEFORE admission extracts preload needs from
// those postings.
//
// Color flows directly from the request into the volume-key tuple admission
// uses for preload extraction, so a malformed value (e.g. `Color="A\x00B"`)
// would otherwise materialize a corrupted cache key before the FSM's own
// ValidateColor rejected the order. Validating here closes that window.
//
// Revert orders need no check here: their reversed postings inherit colors
// from the original transaction (via the coverage-gated TransactionState),
// already validated when that transaction was created.
//
// Postings produced by Numscript still get their second-pass validation in
// the FSM (`validatePostings` in processor_transaction.go); the FSM stays the
// audit-trail enforcement layer for script-resolved postings. This admission
// check only covers the direct-postings shape that bypasses the producer.
func validateOrderPostingColors(order *raftcmdpb.Order) domain.Describable {
	apply, ok := order.GetLedgerScoped().GetPayload().(*raftcmdpb.LedgerScopedOrder_Apply)
	if !ok {
		return nil
	}

	if d, ok := apply.Apply.GetData().(*raftcmdpb.LedgerApplyOrder_CreateTransaction); ok {
		for _, p := range d.CreateTransaction.GetPostings() {
			if err := domain.ValidateColor(p.GetColor()); err != nil {
				return err
			}
		}
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

// metadataWalk carries the visitors walkOrderMetadata invokes. Declared before
// its consumer.
//
// Each visitor returns the failure it wants to surface; a non-nil return stops
// the walk. Neither may mutate what it is handed: the maps are aliased straight
// out of the order, and an accepted order is immutable business intent
// (invariant #10).
type metadataWalk struct {
	// visitMap receives one entity's metadata map. account is the account
	// address for account-scoped maps and "" for the transaction- and
	// ledger-scoped ones, so a visitor can add the account context itself.
	visitMap func(account string, m map[string]*commonpb.MetadataValue) domain.Describable
	// visitKey receives a bare metadata key: the delete-metadata and
	// metadata-field-type orders carry a key with no value.
	visitKey func(key string) domain.Describable
}

// walkOrderMetadata invokes walk's visitors for every place an order can carry
// metadata: the ledger-apply variants, the ledger-metadata orders, and the
// mirror-ingest entries.
//
// This is the single source of truth for *where* metadata lives in an order.
// The shape validation, the size validation, and the per-command byte
// accounting all traverse through it, so the three can never disagree about
// which maps count — a drift would either let a map through unvalidated or make
// the per-command total exclude bytes the per-entity check already saw.
func walkOrderMetadata(order *raftcmdpb.Order, walk metadataWalk) domain.Describable {
	ls := order.GetLedgerScoped()
	if ls == nil {
		return nil
	}

	switch p := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_Apply:
		return walkApplyMetadata(p.Apply, walk)
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		return walk.visitMap("", p.SaveLedgerMetadata.GetMetadata())
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		return walk.visitKey(p.DeleteLedgerMetadata.GetKey())
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		return walkMirrorMetadata(p.MirrorIngest.GetEntry(), walk)
	default:
		return nil
	}
}

// walkApplyMetadata walks the metadata carried by a LedgerApplyOrder.
func walkApplyMetadata(apply *raftcmdpb.LedgerApplyOrder, walk metadataWalk) domain.Describable {
	switch d := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		if err := walk.visitMap("", d.CreateTransaction.GetMetadata()); err != nil {
			return err
		}

		return walkAccountMetadata(d.CreateTransaction.GetAccountMetadata(), walk)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		// processRevertTransaction stores order.GetMetadata() straight into
		// the revert log payload, so the metadata-key invariants (non-empty,
		// no NUL bytes) must be checked here too. Without this gate a
		// client-supplied empty or NUL-bearing key reaches the canonical
		// Pebble key layout via the revert log and corrupts read-index
		// entries (#322).
		return walk.visitMap("", d.RevertTransaction.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		return walk.visitMap("", d.AddMetadata.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		return walk.visitKey(d.DeleteMetadata.GetKey())
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		return walk.visitKey(d.SetMetadataFieldType.GetKey())
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		return walk.visitKey(d.RemoveMetadataFieldType.GetKey())
	default:
		return nil
	}
}

// walkMirrorMetadata walks the metadata supplied by mirror ingest orders.
func walkMirrorMetadata(entry *raftcmdpb.MirrorLogEntry, walk metadataWalk) domain.Describable {
	switch d := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		if err := walk.visitMap("", d.CreatedTransaction.GetMetadata()); err != nil {
			return err
		}

		return walkAccountMetadata(d.CreatedTransaction.GetAccountMetadata(), walk)
	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		return walk.visitMap("", d.SavedMetadata.GetMetadata())
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		return walk.visitMap("", d.RevertedTransaction.GetMetadata())
	default:
		return nil
	}
}

// walkAccountMetadata walks the per-account maps of a transaction order. A nil
// map value carries nothing and is skipped rather than reported: an absent map
// is not a validation failure.
func walkAccountMetadata(accountMetadata map[string]*commonpb.MetadataMap, walk metadataWalk) domain.Describable {
	for account, mm := range accountMetadata {
		if mm == nil {
			continue
		}

		if err := walk.visitMap(account, mm.GetValues()); err != nil {
			return err
		}
	}

	return nil
}

// validateOrderMetadata validates that all metadata keys and values in the order
// are safe for Pebble key encoding. Shape only — the size contract is enforced
// per command by validateCommandMetadata, which needs the replicated ceilings.
func validateOrderMetadata(order *raftcmdpb.Order) domain.Describable {
	return walkOrderMetadata(order, metadataWalk{
		visitMap: func(account string, m map[string]*commonpb.MetadataValue) domain.Describable {
			err := validateMetadataMap(m)
			if err == nil || account == "" {
				return err
			}

			return &domain.ErrAccountValidation{Account: account, Cause: err}
		},
		visitKey: domain.ValidateMetadataKey,
	})
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

	// An executable reference's selector — the literal "latest" or a full semver
	// (omitted and partial selectors are read-only) — is state-independent, so it
	// is gated structurally for every accepted order. Script resolution runs far
	// later, and is short-circuited for an order the fold loop predicts the FSM
	// will skip, so a selector reaching the audit chain is decided here.
	if refValid {
		if v := o.GetNumscriptReference().GetVersion(); v != "latest" {
			if _, err := semver.Parse(v); err != nil {
				return &domain.ErrNumscriptInvalidVersion{Version: v}
			}
		}
	}

	return nil
}

// validateOrderCreateIndex rejects a CreateIndex order for an IndexID the
// builder has no backfill path for — which would otherwise be persisted as a
// registry entry whose backfill never runs or completes. ParseCanonical
// decodes any well-formed IndexID reachable from an HTTP/gRPC create body,
// including unsupported enum values (e.g. metadata:TARGET_TYPE_LEDGER:<key>,
// account_builtin:ACCT_BUILTIN_INDEX_UNSPECIFIED, log_builtin:...UNSPECIFIED),
// so this gate covers every index kind via indexes.Supported.
func validateOrderCreateIndex(order *raftcmdpb.Order) domain.Describable {
	apply, ok := order.GetLedgerScoped().GetPayload().(*raftcmdpb.LedgerScopedOrder_Apply)
	if !ok {
		return nil
	}

	ci, ok := apply.Apply.GetData().(*raftcmdpb.LedgerApplyOrder_CreateIndex)
	if !ok {
		return nil
	}

	if !indexes.Supported(ci.CreateIndex.GetId()) {
		return ErrIndexTargetUnsupported
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

		if err := domain.ValidatePreparedQueryName(q.GetName()); err != nil {
			return err
		}

		// Reject a target the prepared-query executor cannot run (AUDIT, and any
		// non-executable value a gRPC caller could set directly) before it is
		// persisted, and validate each filter condition against that specific
		// target — not the union of REST targets — so an ACCOUNTS query cannot
		// store transaction-only conditions. Both gates share the commonpb
		// validity table with the compile layer (EN-1504).
		if !domain.IsPreparedQueryExecutableTarget(q.GetTarget()) {
			return domain.ErrPreparedQueryTargetUnsupported
		}

		return domain.ValidateFilterForTarget(q.GetFilter(), q.GetTarget())
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

// validateOrderSigningKey rejects a signing-key registration whose public key is
// not a well-formed Ed25519 public key. The stored value layout is
// [publicKey 32B][parentKeyID variable] (state.SaveSigningKey), and the reader
// splits at a hard-coded 32, so any other length either mis-parses the parent ID
// boundary or makes the row undecodable.
func validateOrderSigningKey(order *raftcmdpb.Order) domain.Describable {
	reg, ok := order.GetSystemScoped().GetPayload().(*raftcmdpb.SystemScopedOrder_RegisterSigningKey)
	if !ok {
		return nil
	}

	if len(reg.RegisterSigningKey.GetPublicKey()) != ed25519.PublicKeySize {
		return ErrSigningKeyInvalidLength
	}

	return nil
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

// validateCommandMetadata enforces the metadata size contract over a whole
// command — the batch of orders that becomes one atomic, signed Raft proposal.
//
// It is the single admission-side gate for the size ceilings, so direct HTTP,
// public gRPC, bulk and mirror ingest are all bounded by the same numbers: they
// converge on requestsToOrders, and every metadata-bearing order shape is
// reached through walkOrderMetadata.
//
// The per-entity ceilings are checked order by order, then the accumulated total
// against the per-command ceiling — so a caller cannot defeat the per-entity
// bound by spreading one large payload across many entities in a single command.
// The limits come from the committed cluster policy, which the FSM reads too, so
// admission and apply agree on every node.
func validateCommandMetadata(orders []*raftcmdpb.Order, limits domain.MetadataLimits) error {
	var total uint64

	for _, order := range orders {
		if err := validateOrderMetadataLimits(order, limits); err != nil {
			return &domain.BusinessError{Err: err}
		}

		total += orderMetadataBytes(order)
	}

	if err := limits.ValidateCommandBytes(total); err != nil {
		return &domain.BusinessError{Err: err}
	}

	return nil
}

// validateOrderMetadataLimits checks one order's metadata against the per-entity
// ceilings and the per-key ceiling.
func validateOrderMetadataLimits(order *raftcmdpb.Order, limits domain.MetadataLimits) domain.Describable {
	return walkOrderMetadata(order, metadataWalk{
		visitMap: func(account string, m map[string]*commonpb.MetadataValue) domain.Describable {
			err := limits.ValidateMap(m)
			if err == nil || account == "" {
				return err
			}

			return &domain.ErrAccountValidation{Account: account, Cause: err}
		},
		visitKey: limits.ValidateKey,
	})
}

// orderMetadataBytes is the measured metadata size of one order, summed over
// every map and bare key it carries. Summation is order-independent, so the
// result does not depend on Go map iteration order.
func orderMetadataBytes(order *raftcmdpb.Order) uint64 {
	var total uint64

	// The visitors only accumulate and never fail, so the walk cannot return an
	// error here.
	_ = walkOrderMetadata(order, metadataWalk{
		visitMap: func(_ string, m map[string]*commonpb.MetadataValue) domain.Describable {
			total += domain.MetadataMapSize(m)

			return nil
		},
		visitKey: func(key string) domain.Describable {
			total += uint64(len(key))

			return nil
		},
	})

	return total
}
