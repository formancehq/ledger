package domain

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// FailureFacts is the stable business outcome recorded by audit and
// idempotency. Human-readable text is deliberately absent.
type FailureFacts struct {
	Reason commonpb.ErrorReason
	Code   string
	Facts  map[string]string
}

// FailureFactsOf selects the semantic identity of an FSM error. Wrappers keep
// the underlying code while adding their own structured correlators.
func FailureFactsOf(d SerializableError) FailureFacts {
	facts := d.FailureFacts()
	if expected := ReasonCode(d.Reason()); facts.Reason != expected {
		panic(fmt.Errorf("failure reason %s does not match error reason %s", facts.Reason, expected))
	}

	return facts
}

// NewFailureFacts copies the values supplied by an error into its audited
// projection. Producers must validate their values before constructing it.
func NewFailureFacts(reason, code string, values map[string]string) FailureFacts {
	facts := make(map[string]string, len(values))
	maps.Copy(facts, values)

	return FailureFacts{Reason: ReasonCode(reason), Code: code, Facts: facts}
}

var failureCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// These subcodes disambiguate failures that share the VALIDATION reason. New
// sentinels must be registered here before their outcomes can be audited.
var validationFailureCodes = map[string]struct{}{
	"TARGET_REQUIRED": {}, "METADATA_KEY_REQUIRED": {},
	"NUMSCRIPT_CONTENT_REQUIRED": {}, "SCRIPT_AND_REFERENCE_CONFLICT": {},
	"EMPTY_TRANSACTION": {}, "POSTINGS_AND_SCRIPT_CONFLICT": {},
	"TRANSACTION_REFERENCE_TOO_LONG": {},
	"SCRIPT_REQUIRED":                {}, "NUMSCRIPT_NAME_REQUIRED": {},
	"NUMSCRIPT_NAME_INVALID_CHAR": {}, "NUMSCRIPT_NAME_TOO_LONG": {},
	"SCOPED_BALANCE_UNSUPPORTED": {}, "NUMSCRIPT_SCALING_UNSUPPORTED": {},
	"PREPARED_QUERY_REQUIRED": {}, "PREPARED_QUERY_NAME_REQUIRED": {},
	"PREPARED_QUERY_NAME_INVALID_CHAR": {}, "PREPARED_QUERY_NAME_TOO_LONG": {},
	"PREPARED_QUERY_TARGET_UNSUPPORTED": {}, "SIGNING_KEY_ID_REQUIRED": {},
	"SIGNING_KEY_ID_INVALID_CHAR": {}, "SIGNING_KEY_ID_TOO_LONG": {},
	"COLOR_INVALID": {}, "COLOR_TOO_LONG": {},
	"ACCOUNT_TYPE_NAME_TOO_LONG": {}, "ACCOUNT_TYPE_PATTERN_TOO_LONG": {},
	"TRANSACTION_TARGET_MISSING": {}, "INVALID_SKIPPABLE_REASON": {},
	"MIRROR_HTTP_URL_INVALID": {}, "MIRROR_IAM_REGION_REQUIRED": {},
	"MIRROR_IAM_REQUIRES_TLS": {}, "MIRROR_REWRITE_RULE_INVALID": {},
	"LEDGER_NAME_RESERVED_PREFIX": {}, "INDEX_TARGET_UNSUPPORTED": {},
	"SIGNING_KEY_INVALID_LENGTH":  {},
	"ID_REQUIRED":                 {},
	"DEPENDENCY_DISCOVERY_FAILED": {},
	"LEDGER_NAME_REQUIRED":        {}, "LEDGER_NAME_INVALID_CHAR": {},
	"LEDGER_NAME_TOO_LONG": {}, "METADATA_KEY_EMPTY": {},
	"METADATA_KEY_INVALID_CHAR": {}, "METADATA_VALUE_CONTAINS_NULL_BYTE": {},
	"ACCOUNT_ADDRESS_EMPTY": {}, "ACCOUNT_ADDRESS_INVALID_CHAR": {},
	"ACCOUNT_ADDRESS_EMPTY_SEGMENT": {}, "ACCOUNT_ADDRESS_TOO_LONG": {},
	"ASSET_INVALID": {},
}

var numscriptFailureCodes = map[string]string{
	"NUMSCRIPT_MISSING_FUNDS":                     "asset needed available",
	"NUMSCRIPT_NEGATIVE_AMOUNT":                   "amount",
	"NUMSCRIPT_MISSING_VARIABLE":                  "name",
	"NUMSCRIPT_INVALID_ACCOUNT_NAME":              "name",
	"NUMSCRIPT_INVALID_ASSET":                     "name",
	"NUMSCRIPT_INVALID_COLOR":                     "color",
	"NUMSCRIPT_INVALID_SCOPE":                     "scope",
	"NUMSCRIPT_INVALID_MONETARY_LITERAL":          "source",
	"NUMSCRIPT_INVALID_NUMBER_LITERAL":            "source",
	"NUMSCRIPT_BAD_PORTION":                       "source",
	"NUMSCRIPT_CURRENCY_MISMATCH":                 "expected got",
	"NUMSCRIPT_DIVIDE_BY_ZERO":                    "numerator",
	"NUMSCRIPT_TYPE_ERROR":                        "expected",
	"NUMSCRIPT_METADATA_NOT_FOUND":                "account scope key",
	"NUMSCRIPT_NEGATIVE_BALANCE":                  "account scope asset amount",
	"NUMSCRIPT_INVALID_ALLOTMENT_SUM":             "actualSum",
	"NUMSCRIPT_NEGATIVE_PORTION":                  "portion",
	"NUMSCRIPT_INVALID_REMAINING_ALLOTMENT":       "",
	"NUMSCRIPT_INVALID_ALLOTMENT_IN_SEND_ALL":     "",
	"NUMSCRIPT_INVALID_UNBOUNDED_SEND_ALL":        "name scope",
	"NUMSCRIPT_INVALID_UNBOUNDED_SCALING_ADDRESS": "",
	"NUMSCRIPT_INVALID_NESTED_META":               "",
	"NUMSCRIPT_CANNOT_CAST_TO_STRING":             "",
	"NUMSCRIPT_CANNOT_CAST_SCOPED_ACCOUNT":        "account scope",
	"NUMSCRIPT_CANNOT_STORE_SCOPED_ACCOUNT":       "account scope",
	"NUMSCRIPT_UNBOUND_VARIABLE":                  "name",
	"NUMSCRIPT_UNBOUND_FUNCTION":                  "name",
	"NUMSCRIPT_BAD_ARITY":                         "expectedArity givenArguments",
	"NUMSCRIPT_INVALID_TYPE":                      "name",
	"NUMSCRIPT_EXPERIMENTAL_FEATURE":              "flagName",
	"NUMSCRIPT_INVALID_FEATURE":                   "feature",
	"NUMSCRIPT_ASSET_MISMATCH":                    "expected got",
	"NUMSCRIPT_INVALID_UNCAPPED_SOURCE":           "account",
	"NUMSCRIPT_BAD_META_VALUE":                    "account key",
	"NUMSCRIPT_POSTING_NEGATIVE_AMOUNT":           "postingIndex amount",
	"NUMSCRIPT_POSTING_AMOUNT_OVERFLOW":           "postingIndex amount",
}

var numscriptCompileFailureCodes = map[string]string{
	"NUMSCRIPT_COMPILE_TYPE_MISMATCH":               "expected got",
	"NUMSCRIPT_COMPILE_UNBOUND_VARIABLE":            "name type",
	"NUMSCRIPT_COMPILE_INVALID_TYPE":                "name",
	"NUMSCRIPT_COMPILE_BAD_ARITY":                   "expected actual",
	"NUMSCRIPT_COMPILE_UNKNOWN_FUNCTION":            "name wrongContext",
	"NUMSCRIPT_COMPILE_DUPLICATE_VARIABLE":          "name",
	"NUMSCRIPT_COMPILE_INVALID_UNCAPPED_SOURCE":     "",
	"NUMSCRIPT_COMPILE_DUPLICATE_REMAINING":         "",
	"NUMSCRIPT_COMPILE_INVALID_META_POSITION":       "",
	"NUMSCRIPT_COMPILE_CANNOT_CAST_TO_STRING":       "type",
	"NUMSCRIPT_COMPILE_CANNOT_STORE_SCOPED_ACCOUNT": "",
	"NUMSCRIPT_COMPILE_EXPERIMENTAL_FEATURE":        "flagName",
	"NUMSCRIPT_COMPILE_INVALID_FEATURE":             "feature",
	"NUMSCRIPT_COMPILE_MISSING_VARIABLE":            "name",
	"NUMSCRIPT_COMPILE_INVALID_VARIABLE_VALUE":      "name type",
}

// Allowed fact keys are reason-specific. A changed key changes the business
// identity, so a producer and every reader must agree on the schema.
var failureFactKeys = map[string]string{
	"SEQUENCE_EXHAUSTED":    "counter",
	"LEDGER_ALREADY_EXISTS": "name", "LEDGER_NOT_FOUND": "name",
	"LEDGER_DELETED": "name", "IDEMPOTENCY_KEY_CONFLICT": "key",
	"TRANSACTION_REFERENCE_CONFLICT":  "ledger reference existingTransactionId",
	"TRANSACTION_NOT_FOUND":           "transactionId",
	"TRANSACTION_REFERENCE_NOT_FOUND": "reference",
	"TRANSACTION_ALREADY_REVERTED":    "transactionId",
	"REVERT_TARGET_CREATED_IN_BATCH":  "transactionId",
	"INSUFFICIENT_FUNDS":              "account asset amount balance color colorKnown",
	"VOLUME_OVERFLOW":                 "account asset color side amount current",
	"BALANCE_NOT_FOUND":               "account asset",
	"SINK_ALREADY_EXISTS":             "name", "SINK_NOT_FOUND": "name",
	"SINK_BATCH_SIZE_TOO_LARGE": "name batchSize max",
	"SINK_CONTROLLER_MISMATCH":  "name controllerId",
	"METADATA_NOT_FOUND":        "target key account",
	"INVALID_CRON_EXPRESSION":   "expression",
	"LEDGER_IN_MIRROR_MODE":     "name", "LEDGER_NOT_IN_MIRROR_MODE": "name",
	"MIRROR_V2_LOG_ID_GAP":          "name got expected",
	"MIRROR_V2_LOG_ID_INVALID":      "name",
	"PREPARED_QUERY_ALREADY_EXISTS": "ledger name",
	"PREPARED_QUERY_NOT_FOUND":      "ledger name",
	"INDEX_ALREADY_EXISTS":          "index", "INDEX_NOT_FOUND": "index",
	"INDEX_BUILDING": "index", "INDEX_INCONSISTENT": "index",
	"METADATA_FIELD_NOT_IN_SCHEMA":     "target key",
	"NUMSCRIPT_NOT_FOUND":              "name version",
	"NUMSCRIPT_VERSION_ALREADY_EXISTS": "name version",
	"NUMSCRIPT_INVALID_VERSION":        "version",
	"ACCOUNT_NOT_MATCHING_TYPE":        "address",
	"ACCOUNT_TYPE_NOT_FOUND":           "name", "ACCOUNT_TYPE_ALREADY_EXISTS": "name",
	"ACCOUNT_TYPE_CONFLICT": "pattern existingName existingPattern",
	"INVALID_PATTERN":       "pattern", "ACCOUNT_TYPE_HAS_ACCOUNTS": "name",
	"BALANCE_NOT_PRELOADED":            "account asset color",
	"TRANSIENT_ACCOUNT_NON_ZERO":       "accounts",
	"STALE_CLUSTER_POLICY":             "proposedRevision appliedRevision",
	"CLUSTER_POLICY_INVALID":           "account key",
	"CLUSTER_POLICY_REVISION_CONFLICT": "revision",
	"TRANSACTION_STATE_INCONSISTENT":   "transactionId",
	"CHECKPOINT_NOT_READY":             "checkpointId",
	"CHECKPOINT_LIMIT_REACHED":         "limit",
	"CHECKPOINT_NOT_FOUND":             "checkpointId",
	"METADATA_LIMIT_EXCEEDED":          "dimension limit actual key account",
	"VOLUME_NOT_MATERIALIZED":          "account asset color side",
	"COVERAGE_MISS":                    "attribute canonicalHex idHex raftIndex",
	"NUMSCRIPT_RUNTIME":                "key",
	"EXECUTION_PLAN_TOO_LARGE":         "size limit",
	"AGGREGATE_OVERFLOW":               "stage side",
	"VALIDATION":                       "key account providedReason",
}

var numericFailureFacts = map[string]struct{}{
	"transactionId": {}, "existingTransactionId": {},
	"batchSize": {}, "max": {},
	"proposedRevision": {}, "appliedRevision": {}, "revision": {},
	"checkpointId": {}, "limit": {}, "actual": {}, "size": {},
	"raftIndex":    {},
	"postingIndex": {},
}

// ValidateFailureFacts rejects unknown codes, extra keys, malformed numeric
// values, and text that cannot be rendered consistently across replicas.
func ValidateFailureFacts(f FailureFacts) error {
	if f.Reason == commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED || ReasonString(f.Reason) == "" {
		return fmt.Errorf("failure reason is unknown: %d", f.Reason)
	}
	if !failureCodePattern.MatchString(f.Code) {
		return fmt.Errorf("invalid failure code %q", f.Code)
	}
	reason := ReasonString(f.Reason)
	switch {
	case reason == ErrReasonValidation:
		if _, ok := validationFailureCodes[f.Code]; !ok {
			return fmt.Errorf("unknown validation failure code %q", f.Code)
		}
	case (reason == ErrReasonNumscriptRuntime || reason == ErrReasonNumscriptExecutionError) && f.Code != reason:
		if _, ok := numscriptFailureCodes[f.Code]; !ok {
			return fmt.Errorf("unknown numscript failure code %q", f.Code)
		}
	case reason == ErrReasonNumscriptCompileError && f.Code != reason:
		if _, ok := numscriptCompileFailureCodes[f.Code]; !ok {
			return fmt.Errorf("unknown numscript compile failure code %q", f.Code)
		}
	case f.Code != reason && (reason != ErrReasonClusterPolicyInvalid || f.Code != "METADATA_LIMITS_UNCONFIGURED"):
		return fmt.Errorf("failure code %q does not match reason %q", f.Code, reason)
	}
	allowed := strings.Fields(failureFactKeys[reason])
	if reason == ErrReasonNumscriptRuntime || reason == ErrReasonNumscriptExecutionError {
		allowed = strings.Fields(numscriptFailureCodes[f.Code])
		if f.Code == reason {
			allowed = strings.Fields(failureFactKeys[reason])
		}
	}
	if reason == ErrReasonNumscriptCompileError && f.Code != reason {
		allowed = strings.Fields(numscriptCompileFailureCodes[f.Code])
	}
	for key, value := range f.Facts {
		if !slices.Contains(allowed, key) {
			return fmt.Errorf("fact %q is not allowed for %s", key, reason)
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("fact %q is not valid UTF-8", key)
		}
		_, numeric := numericFailureFacts[key]
		if (reason == ErrReasonMirrorV2LogIDGap || f.Code == "NUMSCRIPT_COMPILE_BAD_ARITY") && (key == "got" || key == "expected") {
			numeric = true
		}
		if numeric {
			if _, err := strconv.ParseUint(value, 10, 64); err != nil {
				return fmt.Errorf("fact %q must be an unsigned integer: %w", key, err)
			}
		}
	}

	return nil
}

// RenderFailureFacts supplies a deterministic presentation at read/API edges.
// A caller may attach its own transient diagnostic to a live error, but that
// text is never part of the audited identity.
func RenderFailureFacts(f FailureFacts) (string, map[string]string) {
	name := strings.ToLower(strings.ReplaceAll(f.Code, "_", " "))
	facts := maps.Clone(f.Facts)
	if len(facts) == 0 {
		return name, facts
	}
	keys := slices.Sorted(maps.Keys(facts))
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strconv.Quote(facts[key]))
	}

	return name + ": " + strings.Join(parts, ", "), facts
}
