package sdk

import "slices"

// FailureCode is the closed vocabulary of safe failure codes that may cross a
// plugin boundary. The code travels as a bare string on every transport
// (protobuf `Failure.code`, the WASM JSON frame), so each spelling below is
// wire contract: changing one breaks every surface that matches on it.
//
// The vocabulary lives here, beside Failure itself, because pkg/plugin is a
// separate module that plugin runtimes import; a host-only home could not close
// the set while runtime adapters construct codes of their
// own.
type FailureCode string

const (
	// FailureSetupRequired states that a profile, capability, or target binding
	// could not be resolved. It is raised before any network access.
	FailureSetupRequired FailureCode = "setup_required"
	// FailureServiceInfoUnavailable states that the bounded public probe did not
	// yield a usable response. It never carries a response body.
	FailureServiceInfoUnavailable FailureCode = "service_info_unavailable"
	// FailureServiceVersionInvalid states that a probe completed but the version
	// was absent, malformed, or not SemVer.
	FailureServiceVersionInvalid FailureCode = "service_version_invalid"
	// FailureExecutionContextInvalid states that a prepared execution context
	// contradicts its locked command before an adapter starts.
	FailureExecutionContextInvalid FailureCode = "execution_context_invalid"
	// FailurePluginIncompatible states that the live product major is not
	// supported by the selected command.
	FailurePluginIncompatible FailureCode = "plugin_incompatible"
	// FailureOperationNotPermitted states that the operation is absent from the
	// verified catalogue. It is refused before any resolver or network call.
	FailureOperationNotPermitted FailureCode = "operation_not_permitted"
	// FailureCanceled states that the caller cancelled or a deadline elapsed.
	FailureCanceled FailureCode = "canceled"
	// FailureProtocolError states that a plugin violated the frame contract.
	FailureProtocolError FailureCode = "protocol_error"
	// FailureProtocolInvalidResultCount states that an execution emitted other
	// than exactly one result envelope.
	FailureProtocolInvalidResultCount FailureCode = "protocol_invalid_result_count"
	// FailureBudgetExhausted states that a portable execution crossed a
	// normative resource limit. No partial result accompanies it and the host
	// destroys the execution instance before admitting a fresh execution.
	FailureBudgetExhausted FailureCode = "budget_exhausted"
	// FailureProductRequestFailed states that a product call failed at the
	// transport layer.
	FailureProductRequestFailed FailureCode = "product_request_failed"
	// FailureProductResponseFailed states that a product response could not be
	// read, bounded, or validated.
	FailureProductResponseFailed FailureCode = "product_response_failed"
	// FailureProductHTTPError states that a product returned a non-2xx status.
	FailureProductHTTPError FailureCode = "product_http_error"
	// FailureProductGRPCError states that a product returned a non-OK status.
	FailureProductGRPCError FailureCode = "product_grpc_error"
	// FailureInternal is the redacted code for a host-side or upstream fault. It
	// never carries diagnostic detail across the boundary.
	FailureInternal FailureCode = "internal"
	// FailureRequestAuthorizationFailed states that a resolved credential could
	// not be applied to a request.
	FailureRequestAuthorizationFailed FailureCode = "request_authorization_failed"
	// FailureCompanionRequestFailed states that the notebook companion refused
	// or could not serve a request.
	FailureCompanionRequestFailed FailureCode = "companion_request_failed"
	// FailureBrokerFailed states that a credential broker operation failed.
	FailureBrokerFailed FailureCode = "broker_failed"
	// FailureHostRequestFailed states that host-side dispatch of Host.Request
	// failed.
	FailureHostRequestFailed FailureCode = "host_request_failed"
	// FailureExecutionFailed is the default for a plugin execution error that
	// declares no more precise code.
	FailureExecutionFailed FailureCode = "execution_failed"
	// FailureTargetDiscoveryFailed states that the target provider facet failed.
	FailureTargetDiscoveryFailed FailureCode = "target_discovery_failed"
	// FailureAuthResolutionFailed states that the auth provider facet failed.
	FailureAuthResolutionFailed FailureCode = "auth_resolution_failed"
	// FailureSignerResolutionFailed states that the signer facet failed.
	FailureSignerResolutionFailed FailureCode = "signer_resolution_failed"
	// FailureSigningFailed states that serialization, key binding, or signature
	// production failed. After signer activation the host never retries unsigned.
	FailureSigningFailed FailureCode = "signing_failed"
	// FailureOutputSchemaInvalid states that a result did not validate against
	// the declared public output schema.
	FailureOutputSchemaInvalid FailureCode = "output_schema_invalid"
	// FailureInvalidArgument states that a caller-supplied value was rejected
	// before any product call: an unknown output format, an unsupported media
	// type, or a malformed argument.
	FailureInvalidArgument FailureCode = "invalid_argument"
	// FailureSensitiveDeliveryFailed states that a one-shot sensitive result
	// could not be delivered: already read, discarded, or delivered in a mode
	// the operation does not declare.
	FailureSensitiveDeliveryFailed FailureCode = "sensitive_delivery_failed"
	// FailurePaginationLimitExceeded states that a traversal crossed a declared
	// page, item, or byte ceiling. No partial result accompanies it.
	FailurePaginationLimitExceeded FailureCode = "pagination_limit_exceeded"
	// FailureUnsupportedPagination states that a caller asked to traverse an
	// operation whose descriptor declares no pagination.
	FailureUnsupportedPagination FailureCode = "unsupported_pagination"
	// FailureInputSourceConflict states that other than one declared input
	// source was selected.
	FailureInputSourceConflict FailureCode = "input_source_conflict"
	// FailureInputTooLarge states that an input artifact exceeds its declared
	// ceiling.
	FailureInputTooLarge FailureCode = "input_too_large"
	// FailureInputMediaTypeInvalid states that an input media type is not one
	// the operation declares.
	FailureInputMediaTypeInvalid FailureCode = "input_media_type_invalid"
	// FailureDescriptorInvalid states that a descriptor declared something the
	// host does not accept, such as a signing capability it never approved.
	FailureDescriptorInvalid FailureCode = "descriptor_invalid"
	// FailureSigningContractInvalid states that a signing request violated the
	// approved contract at execution time.
	FailureSigningContractInvalid FailureCode = "signing_contract_invalid"
	// FailureAuthScopeDenied states that requested scopes exceed the host
	// ceiling. No token is requested for a denied scope.
	FailureAuthScopeDenied FailureCode = "auth_scope_denied"
	// FailureCompletionLimitExceeded states that a completion response crossed a
	// declared ceiling. No candidate is presented rather than a truncated list.
	FailureCompletionLimitExceeded FailureCode = "completion_limit_exceeded"
	// FailureCompletionResponseInvalid states that a completion response cannot
	// be presented, such as one value described two contradictory ways.
	FailureCompletionResponseInvalid FailureCode = "completion_response_invalid"
	// FailureDescriptorLimitExceeded states that a descriptor declared more than
	// a configured ceiling permits.
	FailureDescriptorLimitExceeded FailureCode = "descriptor_limit_exceeded"
)

// failureCodes is the authoritative closed set, in declaration order.
var failureCodes = []FailureCode{
	FailureSetupRequired,
	FailureServiceInfoUnavailable,
	FailureServiceVersionInvalid,
	FailureExecutionContextInvalid,
	FailurePluginIncompatible,
	FailureOperationNotPermitted,
	FailureCanceled,
	FailureProtocolError,
	FailureProtocolInvalidResultCount,
	FailureBudgetExhausted,
	FailureProductRequestFailed,
	FailureProductResponseFailed,
	FailureProductHTTPError,
	FailureProductGRPCError,
	FailureInternal,
	FailureRequestAuthorizationFailed,
	FailureCompanionRequestFailed,
	FailureBrokerFailed,
	FailureHostRequestFailed,
	FailureExecutionFailed,
	FailureTargetDiscoveryFailed,
	FailureAuthResolutionFailed,
	FailureSignerResolutionFailed,
	FailureSigningFailed,
	FailureOutputSchemaInvalid,
	FailureInvalidArgument,
	FailureSensitiveDeliveryFailed,
	FailurePaginationLimitExceeded,
	FailureUnsupportedPagination,
	FailureInputSourceConflict,
	FailureInputTooLarge,
	FailureInputMediaTypeInvalid,
	FailureDescriptorInvalid,
	FailureSigningContractInvalid,
	FailureAuthScopeDenied,
	FailureCompletionLimitExceeded,
	FailureCompletionResponseInvalid,
	FailureDescriptorLimitExceeded,
}

// FailureCodes returns the closed vocabulary. The result is a copy, so a caller
// cannot widen the set.
func FailureCodes() []FailureCode {
	return slices.Clone(failureCodes)
}

// Valid reports whether code is declared. An undeclared code is never admitted:
// no normalisation, trimming, or case folding is attempted, because a code that
// needs repair did not come from this vocabulary.
func (code FailureCode) Valid() bool {
	return slices.Contains(failureCodes, code)
}
