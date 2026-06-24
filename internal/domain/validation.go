package domain

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// maxLedgerNameLength is the maximum allowed length for a ledger name.
// Capped to dal.LedgerNameFixedSize because every ledger-scoped canonical
// key reserves exactly that many bytes for the name (zero-padded).
// Validating upstream prevents silent truncation in appendLedgerName,
// which would otherwise cause key collisions between names sharing the
// first N bytes.
const maxLedgerNameLength = dal.LedgerNameFixedSize

// maxNumscriptNameLength caps numscript identifiers. Same envelope as
// ledger names — they are addressed the same way in CLI/UI/RPC.
const maxNumscriptNameLength = 256

// maxPreparedQueryNameLength caps prepared-query identifiers. Same envelope
// as numscript names — they are addressed the same way in CLI/UI/RPC.
const maxPreparedQueryNameLength = 256

// maxSigningKeyIDLength caps signing-key identifiers. Operators usually
// reuse a short slug ("admin-key-1"); the same 256-byte envelope as the
// other named resources is plenty.
const maxSigningKeyIDLength = 256

// maxAccountAddressLength is the maximum allowed length for an account address.
const maxAccountAddressLength = 1024

// Storage-safety validation sentinels. All are Describable so they flow
// through BusinessError with Kind=KindValidation.
var (
	ErrLedgerNameContainsNullByte          = newValidationSentinel("ledger name must not contain null bytes")
	ErrLedgerNameInvalidChar               = newValidationSentinel("ledger name must contain only printable ASCII (0x20–0x7E)")
	ErrLedgerNameTooLong                   = newValidationSentinel(fmt.Sprintf("ledger name exceeds maximum length of %d bytes", maxLedgerNameLength))
	ErrNumscriptNameInvalidChar            = newValidationSentinel("numscript name must contain only printable ASCII (0x20–0x7E)")
	ErrNumscriptNameTooLong                = newValidationSentinel(fmt.Sprintf("numscript name exceeds maximum length of %d bytes", maxNumscriptNameLength))
	ErrPreparedQueryNameRequired           = newValidationSentinel("prepared query name is required")
	ErrPreparedQueryNameInvalidChar        = newValidationSentinel("prepared query name must contain only printable ASCII (0x20–0x7E)")
	ErrPreparedQueryNameTooLong            = newValidationSentinel(fmt.Sprintf("prepared query name exceeds maximum length of %d bytes", maxPreparedQueryNameLength))
	ErrPreparedQueryRequired               = newValidationSentinel("prepared query payload is required")
	ErrPreparedQueryAuditTargetUnsupported = newValidationSentinel("prepared queries do not support the audit target; query the audit trail via ListAuditEntries")
	ErrSigningKeyIDRequired                = newValidationSentinel("signing key id is required")
	ErrSigningKeyIDInvalidChar             = newValidationSentinel("signing key id must contain only printable ASCII (0x20–0x7E)")
	ErrSigningKeyIDTooLong                 = newValidationSentinel(fmt.Sprintf("signing key id exceeds maximum length of %d bytes", maxSigningKeyIDLength))
	ErrMetadataKeyContainsNullByte         = newValidationSentinel("metadata key must not contain null bytes")
	ErrMetadataKeyEmpty                    = newValidationSentinel("metadata key must not be empty")
	ErrMetadataValueContainsNullByte       = newValidationSentinel("metadata value must not contain null bytes")
	ErrAccountAddressEmpty                 = newValidationSentinel("account address must not be empty")
	ErrAccountAddressInvalidChar           = newValidationSentinel("account address must contain only letters, digits, colons, underscores, and hyphens")
	ErrAccountAddressTooLong               = newValidationSentinel(fmt.Sprintf("account address exceeds maximum length of %d bytes", maxAccountAddressLength))

	ErrAssetInvalid = newValidationSentinel("asset must match [A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?(/[1-9][0-9]{0,2})? with precision in [1, 255]")
)

// isPrintableASCII reports whether every byte of s is in the printable ASCII
// range 0x20–0x7E (space through tilde). The bound matches the safe-value
// subset accepted by gRPC metadata headers (HTTP/2 fields strip CR/LF,
// reject control bytes, and have no defined encoding for high bytes), so
// any identifier we plan to round-trip through `x-next-cursor` trailers
// must satisfy this predicate.
func isPrintableASCII(s string) bool {
	for i := range len(s) {
		b := s[i]
		if b < 0x20 || b > 0x7E {
			return false
		}
	}

	return true
}

// ValidateLedgerName checks that a ledger name is safe for use in Pebble key
// encoding AND for transport through gRPC metadata trailers (paginated list
// cursors are derived from the ledger name). Null bytes would corrupt
// null-terminated key layouts; control or high bytes would break the
// `x-next-cursor` resume token. Length is capped to keep keys reasonable.
func ValidateLedgerName(name string) Describable {
	if name == "" {
		return ErrLedgerNameRequired
	}

	if strings.ContainsRune(name, 0) {
		return ErrLedgerNameContainsNullByte
	}

	if !isPrintableASCII(name) {
		return ErrLedgerNameInvalidChar
	}

	if len(name) > maxLedgerNameLength {
		return ErrLedgerNameTooLong
	}

	return nil
}

// ValidateNumscriptName mirrors ValidateLedgerName: numscript names are the
// resume-cursor key for `numscripts list` pagination and must survive the
// same gRPC metadata round-trip.
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
// pagination and must survive the same gRPC metadata round-trip.
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

// ValidatePreparedQueryTarget rejects QUERY_TARGET_AUDIT. The audit trail is
// served only through ListAuditEntries (which compiles QueryFilter_Audit via
// query.CompileAuditPredicate); the prepared-query execution path does not
// dispatch on the audit target, so an audit-targeted prepared query would
// otherwise be admitted and silently return an empty cursor. The enum value
// still exists for parity with the other targets (see misc/proto/common.proto),
// so it is rejected at admission/FSM rather than removed from the wire.
func ValidatePreparedQueryTarget(target commonpb.QueryTarget) Describable {
	if target == commonpb.QueryTarget_QUERY_TARGET_AUDIT {
		return ErrPreparedQueryAuditTargetUnsupported
	}

	return nil
}

// ValidateSigningKeyID mirrors ValidateLedgerName: the key ID lands in the
// `x-next-cursor` trailer of `signing keys list` pagination, so it must be
// safe for HTTP/2 header values (printable ASCII, bounded length).
// Parent key IDs go through the same rule so revoke/cascade traversals
// cannot smuggle in an unsafe identifier either.
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

// isAccountAddressChar returns true if the rune is allowed in an account address.
// Segments are [a-zA-Z0-9_-]+, separated by colons.
func isAccountAddressChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == ':' || r == '_' || r == '-'
}

// ValidateAccountAddress checks that an account address contains only allowed characters
// (letters, digits, colons, underscores, hyphens) and is within length limits.
func ValidateAccountAddress(address string) Describable {
	if address == "" {
		return ErrAccountAddressEmpty
	}

	if len(address) > maxAccountAddressLength {
		return ErrAccountAddressTooLong
	}

	for _, r := range address {
		if !isAccountAddressChar(r) {
			return ErrAccountAddressInvalidChar
		}
	}

	return nil
}

// ValidateMetadataKey checks that a metadata key is safe for use in Pebble key encoding.
// Null bytes would corrupt null-terminated key layouts used in canonical keys and the read index.
func ValidateMetadataKey(key string) Describable {
	if key == "" {
		return ErrMetadataKeyEmpty
	}

	if strings.ContainsRune(key, 0) {
		return ErrMetadataKeyContainsNullByte
	}

	return nil
}

// ValidateMetadataStringValue checks that a metadata string payload is safe for
// null-terminated Pebble key encodings used by the metadata read index.
func ValidateMetadataStringValue(value string) Describable {
	if strings.ContainsRune(value, 0) {
		return ErrMetadataValueContainsNullByte
	}

	return nil
}

// ValidateMetadataValue checks string-bearing metadata values for key-encoding safety.
func ValidateMetadataValue(value *commonpb.MetadataValue) Describable {
	switch v := value.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return ValidateMetadataStringValue(v.StringValue)
	case *commonpb.MetadataValue_NullValue:
		if v.NullValue == nil {
			return nil
		}

		return ValidateMetadataStringValue(v.NullValue.GetOriginal())
	default:
		return nil
	}
}

// ValidateAsset checks that an asset string matches the expected format:
// [A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?(/[1-9]\d{0,2})?
// Examples: "USD", "EUR/2", "BTC/8", "CUSTOM_TOKEN/6".
//
// Precision rules are tight on purpose: the canonical volume key in keys.go
// encodes the precision as a single byte, and ParseAssetPrecision relies on
// validation to have rejected anything that would lose information. Without
// these rules, "USD", "USD/0", "USD/02", and "USD/256" all collapse onto
// the same Pebble/cache volume entry — cross-asset fund contamination in a
// double-entry ledger (#303).
func ValidateAsset(asset string) Describable {
	if len(asset) == 0 {
		return ErrAssetInvalid
	}

	base, precisionStr, hasPrecision := strings.Cut(asset, "/")

	if !validateAssetBase(base) {
		return ErrAssetInvalid
	}

	if hasPrecision && !validateAssetPrecision(precisionStr) {
		return ErrAssetInvalid
	}

	return nil
}

// validateAssetPrecision enforces a canonical, uint8-safe precision suffix:
//   - 1 to 3 digits (max numeric value 255 fits in 3 chars).
//   - no leading zero (rejects "02" → 2 aliasing).
//   - numeric value in [1, 255]; "0" is rejected so "USD/0" cannot alias "USD",
//     and values that overflow uint8 are rejected so "USD/256+" cannot alias
//     anything either.
func validateAssetPrecision(s string) bool {
	if len(s) == 0 || len(s) > 3 {
		return false
	}

	if s[0] == '0' {
		return false
	}

	for i := range s {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	// strconv.Atoi is safe: we've already proven s is all-digit, ≤ 3 chars.
	v, _ := strconv.Atoi(s)

	return v >= 1 && v <= 255
}

// validateAssetBase checks the base part: [A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?
func validateAssetBase(base string) bool {
	if len(base) == 0 {
		return false
	}

	head, tail, hasUnderscore := strings.Cut(base, "_")

	if !isUpperAlphaStart(head, 17) {
		return false
	}

	if hasUnderscore && !isUpperAlpha(tail, 1, 16) {
		return false
	}

	return true
}

func isUpperAlphaStart(s string, maxLen int) bool {
	if len(s) == 0 || len(s) > maxLen {
		return false
	}

	if s[0] < 'A' || s[0] > 'Z' {
		return false
	}

	for i := 1; i < len(s); i++ {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}

	return true
}

func isUpperAlpha(s string, minLen, maxLen int) bool {
	if len(s) < minLen || len(s) > maxLen {
		return false
	}

	for i := range s {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}

	return true
}

// ParseAssetPrecision splits an asset string into its base name and precision.
// "USD/4" → ("USD", 4), "EUR" → ("EUR", 0).
func ParseAssetPrecision(asset string) (string, uint8) {
	base, precStr, found := strings.Cut(asset, "/")
	if !found {
		return asset, 0
	}

	prec, _ := strconv.ParseUint(precStr, 10, 8)

	return base, uint8(prec)
}

// FormatAsset reconstructs an asset string from base and precision.
// ("USD", 4) → "USD/4", ("EUR", 0) → "EUR".
func FormatAsset(base string, precision uint8) string {
	if precision == 0 {
		return base
	}

	return fmt.Sprintf("%s/%d", base, precision)
}
