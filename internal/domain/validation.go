package domain

import (
	"errors"
	"fmt"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// maxNumscriptNameLength caps numscript identifiers. Numscript is a
// ledger-internal DSL — the bound stays here rather than in
// github.com/formancehq/invariants (which only carries Formance-wide
// invariants).
const maxNumscriptNameLength = 256

// maxPreparedQueryNameLength caps prepared-query identifiers. Prepared queries
// are a ledger-internal feature (CQRS read-side); the bound stays local for
// the same reason as the numscript-name bound above.
const maxPreparedQueryNameLength = 256

// maxSigningKeyIDLength caps signing-key identifiers. Request signing is a
// ledger-internal feature; the bound stays local for the same reason as
// the numscript-name bound above.
const maxSigningKeyIDLength = 256

// MaxFilterDepth bounds the recursion depth of any walk over a QueryFilter
// proto tree. A hostile client can hand-craft a deeply-nested filter (e.g. 100k
// repetitions of and/or/not) and submit it via gRPC; without a depth cap a
// recursive walk stack-overflows and aborts the process (Go stack overflow is
// not catchable by recover) — a fatal DoS (#341). 100 is well above any
// legitimate query.
//
// This is the single source of truth for the bound: query.Compile /
// query.CompileAuditFilter apply it at execute time, and ValidateFilterForTarget
// applies it at prepared-query write time. Write-time validation MUST cap at the
// same value so a stored filter that passes validation is always executable —
// otherwise a deeper-but-valid tree is accepted and persisted, then every
// execution fails with the too-deep error (and the deep write itself reopens the
// #341 stack-exhaustion path).
const MaxFilterDepth = 100

// ErrFilterTooDeep is returned when a QueryFilter recursion exceeds
// MaxFilterDepth. Typed Describable (Kind=Validation via ErrFilterCompilation)
// so the gRPC adapter maps it to InvalidArgument with the depth in the message.
// Single source of truth: both query.Compile (execute time) and
// ValidateFilterForTarget (prepared-query write time) return this sentinel.
var ErrFilterTooDeep Describable = &BusinessError{Err: &ErrFilterCompilation{
	Detail: fmt.Sprintf("query filter exceeds maximum nesting depth (%d)", MaxFilterDepth),
}}

// errValidation wraps a primitive validation error from
// github.com/formancehq/invariants so it satisfies the local
// Describable contract (Kind=KindValidation, Reason=ErrReasonValidation)
// without duplicating message strings. Each sentinel below is
// pre-instantiated once, so errors.Is comparisons on the exported variables
// remain stable.
type errValidation struct {
	err error
}

func (e *errValidation) Error() string             { return e.err.Error() }
func (e *errValidation) Unwrap() error             { return e.err }
func (*errValidation) Reason() string              { return ErrReasonValidation }
func (*errValidation) Metadata() map[string]string { return nil }

// Storage-safety validation sentinels. All are Describable so they flow
// through BusinessError. Each one wraps the matching primitive sentinel from
// github.com/formancehq/invariants; the wrapping preserves
// errors.Is identity against the primitive (via Unwrap) and against the
// local sentinel (via pointer identity in wrapValidationErr).
var (
	ErrLedgerNameRequired    Describable = &errValidation{err: invariants.ErrLedgerNameRequired}
	ErrLedgerNameInvalidChar Describable = &errValidation{err: invariants.ErrLedgerNameInvalidChar}
	ErrLedgerNameTooLong     Describable = &errValidation{err: invariants.ErrLedgerNameTooLong}

	ErrMetadataKeyEmpty              Describable = &errValidation{err: invariants.ErrMetadataKeyEmpty}
	ErrMetadataKeyInvalidChar        Describable = &errValidation{err: invariants.ErrMetadataKeyInvalidChar}
	ErrMetadataValueContainsNullByte Describable = &errValidation{err: invariants.ErrMetadataValueContainsNullByte}

	ErrAccountAddressEmpty        Describable = &errValidation{err: invariants.ErrLedgerAccountAddressEmpty}
	ErrAccountAddressInvalidChar  Describable = &errValidation{err: invariants.ErrLedgerAccountAddressInvalidChar}
	ErrAccountAddressEmptySegment Describable = &errValidation{err: invariants.ErrLedgerAccountAddressEmptySegment}
	ErrAccountAddressTooLong      Describable = &errValidation{err: invariants.ErrLedgerAccountAddressTooLong}

	ErrAssetInvalid Describable = &errValidation{err: invariants.ErrAssetInvalid}
)

// wrapValidationErr maps a primitive validation error returned by
// github.com/formancehq/invariants to the matching Describable
// sentinel exported above. Returning the pre-instantiated sentinel
// preserves errors.Is identity for call sites that compare against the
// local variable.
func wrapValidationErr(err error) Describable {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, invariants.ErrLedgerNameRequired):
		return ErrLedgerNameRequired
	case errors.Is(err, invariants.ErrLedgerNameInvalidChar):
		return ErrLedgerNameInvalidChar
	case errors.Is(err, invariants.ErrLedgerNameTooLong):
		return ErrLedgerNameTooLong

	case errors.Is(err, invariants.ErrMetadataKeyEmpty):
		return ErrMetadataKeyEmpty
	case errors.Is(err, invariants.ErrMetadataKeyInvalidChar):
		return ErrMetadataKeyInvalidChar
	case errors.Is(err, invariants.ErrMetadataValueContainsNullByte):
		return ErrMetadataValueContainsNullByte

	case errors.Is(err, invariants.ErrLedgerAccountAddressEmpty):
		return ErrAccountAddressEmpty
	case errors.Is(err, invariants.ErrLedgerAccountAddressInvalidChar):
		return ErrAccountAddressInvalidChar
	case errors.Is(err, invariants.ErrLedgerAccountAddressEmptySegment):
		return ErrAccountAddressEmptySegment
	case errors.Is(err, invariants.ErrLedgerAccountAddressTooLong):
		return ErrAccountAddressTooLong

	case errors.Is(err, invariants.ErrAssetInvalid):
		return ErrAssetInvalid
	}

	// Any unrecognized error from invariants would indicate the lib introduced
	// a sentinel this wrapper hasn't mapped yet — surface loudly instead of
	// silently degrading.
	return &errValidation{err: err}
}

// ValidateLedgerName delegates to invariants and maps the primitive sentinel
// back to the local Describable counterpart.
func ValidateLedgerName(name string) Describable {
	return wrapValidationErr(invariants.ValidateLedgerName(name))
}

// ValidateNumscriptName checks a numscript identifier against the same
// HTTP/2-trailer envelope as ledger names. Numscript is a ledger-internal
// concept so the rule lives here rather than in
// github.com/formancehq/invariants.
//
// Names land in the `x-next-cursor` trailer of the `numscripts list` stream,
// so they must be printable ASCII (0x20–0x7E) and bounded.
func ValidateNumscriptName(name string) Describable {
	if name == "" {
		return ErrNumscriptNameRequired
	}

	if !isPrintableASCII(name) {
		return ErrNumscriptNameInvalidChar
	}

	if len(name) > maxNumscriptNameLength {
		return ErrNumscriptNameTooLong
	}

	return nil
}

// ValidatePreparedQueryName mirrors ValidateNumscriptName for prepared-query
// identifiers. They are the resume-cursor key for `prepared queries list`
// pagination and must survive the same gRPC metadata round-trip. Prepared
// queries are a ledger-internal feature (CQRS read-side), so the rule lives
// here rather than in github.com/formancehq/invariants.
//
// After moving the ledger off `PreparedQuery` onto the surrounding
// `CreatePreparedQueryRequest` (PR #522), a request with a valid top-level
// ledger but a missing/empty `query` no longer fails at `loadLedger("")`;
// it would silently persist an empty-named prepared query. Calling this
// validator at admission/FSM closes that hole loudly.
func ValidatePreparedQueryName(name string) Describable {
	if name == "" {
		return ErrPreparedQueryNameRequired
	}

	if !isPrintableASCII(name) {
		return ErrPreparedQueryNameInvalidChar
	}

	if len(name) > maxPreparedQueryNameLength {
		return ErrPreparedQueryNameTooLong
	}

	return nil
}

// IsPreparedQueryExecutableTarget reports whether a QueryTarget can back a
// prepared query today. Prepared-query execution (query.Execute) hydrates only
// account and transaction results into PreparedQueryCursor; LOGS and AUDIT route
// through paths the prepared-query executor does not implement (ListLogs /
// CompileAuditFilter), and AUDIT is additionally not covered by the public
// target JSON mapping — so a prepared query stored on those targets would fail
// later at execute/marshal time. Enforced at write time (admission + FSM) across
// gRPC and HTTP so a persisted prepared query is always executable. LOGS becomes
// executable with EN-1503.
func IsPreparedQueryExecutableTarget(target commonpb.QueryTarget) bool {
	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return true
	default:
		return false
	}
}

// ValidateFilterForTarget walks a QueryFilter tree and returns the first leaf
// condition that is not valid on `target`, per the generated per-target
// validity table (commonpb.ConditionValidForTarget) — the same gate
// query.compile applies per-condition at execute time. Combinators
// ($and/$or/$not) are structural and always valid; the walk recurses into their
// children. Used at prepared-query write time so a stored filter is always
// executable against its target: a condition invalid on the target can never be
// satisfied and would otherwise silently widen/empty results at execute time
// (invariant #7). A nil filter is treated as nothing to validate — callers
// enforce filter presence separately.
//
// The recursion is bounded by MaxFilterDepth — the same cap query.Compile
// enforces at execute time — so a maliciously (or accidentally) deep tree is
// rejected at write time with ErrFilterTooDeep instead of being persisted (only
// to fail every execution) or overflowing the Go stack on the write path (#341).
func ValidateFilterForTarget(f *commonpb.QueryFilter, target commonpb.QueryTarget) Describable {
	return validateFilterForTarget(f, target, 0)
}

func validateFilterForTarget(f *commonpb.QueryFilter, target commonpb.QueryTarget, depth int) Describable {
	if f == nil {
		return nil
	}

	if depth >= MaxFilterDepth {
		return ErrFilterTooDeep
	}

	switch v := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, sub := range v.And.GetFilters() {
			if err := validateFilterForTarget(sub, target, depth+1); err != nil {
				return err
			}
		}

		return nil
	case *commonpb.QueryFilter_Or:
		for _, sub := range v.Or.GetFilters() {
			if err := validateFilterForTarget(sub, target, depth+1); err != nil {
				return err
			}
		}

		return nil
	case *commonpb.QueryFilter_Not:
		return validateFilterForTarget(v.Not.GetFilter(), target, depth+1)
	}

	kind := commonpb.ConditionKindOf(f)
	if commonpb.ConditionValidForTarget(target, kind) {
		return nil
	}

	return &BusinessError{Err: &ErrFilterCompilation{
		Detail: fmt.Sprintf("condition %q is not valid on %s queries",
			kind.String(), commonpb.TargetHumanName(target)),
	}}
}

// ValidateSigningKeyID checks a signing-key identifier against the same
// HTTP/2-trailer envelope as ledger names. Request signing is a
// ledger-internal feature so the rule lives here rather than in
// github.com/formancehq/invariants. Parent key IDs go through the
// same rule so revoke/cascade traversals cannot smuggle in an unsafe
// identifier either.
func ValidateSigningKeyID(id string) Describable {
	if id == "" {
		return ErrSigningKeyIDRequired
	}

	if !isPrintableASCII(id) {
		return ErrSigningKeyIDInvalidChar
	}

	if len(id) > maxSigningKeyIDLength {
		return ErrSigningKeyIDTooLong
	}

	return nil
}

// isPrintableASCII reports whether every byte of s is in the printable ASCII
// range 0x20–0x7E. Kept local for the numscript / prepared-query /
// signing-key name checks (the invariants lib has its own copy used for
// ledger name validation).
func isPrintableASCII(s string) bool {
	for i := range len(s) {
		b := s[i]
		if b < 0x20 || b > 0x7E {
			return false
		}
	}

	return true
}

// ValidateAccountAddress delegates to invariants and maps the primitive
// sentinel back to the local Describable counterpart.
func ValidateAccountAddress(address string) Describable {
	return wrapValidationErr(invariants.ValidateLedgerAccountAddress(address))
}

// ValidateMetadataKey delegates to invariants and maps the primitive sentinel
// back to the local Describable counterpart.
func ValidateMetadataKey(key string) Describable {
	return wrapValidationErr(invariants.ValidateMetadataKey(key))
}

// ValidateMetadataString validates a string-bearing metadata payload (e.g. a
// numscript-emitted string value before it is wrapped in a MetadataValue).
// It delegates to invariants and maps the primitive sentinel back to the
// local Describable counterpart.
func ValidateMetadataString(value string) Describable {
	return wrapValidationErr(invariants.ValidateMetadataString(value))
}

// ValidateMetadataValue inspects the proto MetadataValue and validates the
// string-bearing variants against the same null-byte rule as keys. Non-string
// variants are accepted unchanged.
func ValidateMetadataValue(value *commonpb.MetadataValue) Describable {
	switch v := value.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return wrapValidationErr(invariants.ValidateMetadataString(v.StringValue))
	case *commonpb.MetadataValue_NullValue:
		if v.NullValue == nil {
			return nil
		}

		return wrapValidationErr(invariants.ValidateMetadataString(v.NullValue.GetOriginal()))
	default:
		return nil
	}
}

// ValidateAsset delegates to invariants and maps the primitive sentinel back
// to the local Describable counterpart.
func ValidateAsset(asset string) Describable {
	return wrapValidationErr(invariants.ValidateAsset(asset))
}

// ParseAssetPrecision re-exports invariants.ParseAssetPrecision so existing callers
// (keys.go, etc.) keep their current import path.
var ParseAssetPrecision = invariants.ParseAssetPrecision

// FormatAsset re-exports invariants.FormatAsset so existing callers keep their
// current import path.
var FormatAsset = invariants.FormatAsset
