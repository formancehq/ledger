package productgrpcmessage

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func mapHostError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var value sdk.Failure
	if !errors.As(err, &value) {
		var pointer *sdk.Failure
		if !errors.As(err, &pointer) || pointer == nil {
			return failure(sdk.FailureHostRequestFailed, "host request failed")
		}
		value = *pointer
	}
	if value.Code == string(sdk.FailureProductGRPCError) {
		if canonicalProductFailure(value) {
			value.Details = append([]byte(nil), value.Details...)
			return value
		}
		return failure(sdk.FailureInternal, "invalid sanitized product gRPC failure")
	}
	if !sdk.FailureCode(value.Code).Valid() {
		return failure(sdk.FailureHostRequestFailed, "host request failed")
	}
	// Preserve the closed failure category, never upstream text or details.
	return failure(sdk.FailureCode(value.Code), "host request failed")
}

func canonicalProductFailure(value sdk.Failure) bool {
	if value.Message != "product gRPC request failed" {
		return false
	}
	// Numeric gRPC codes are wire values, independent of any grpc-go runtime.
	names := [...]string{"CANCELED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED"}
	for i, name := range names {
		code := i + 1
		if string(value.Details) == fmt.Sprintf(`{"grpc_code":%d,"grpc_name":%q}`, code, name) {
			return value.Retryable == (code == 4 || code == 14)
		}
	}
	return false
}
