package grpc

import (
	"errors"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
)

const errorDomain = "ledger"

// errorMapping defines how a business error maps to a gRPC status.
type errorMapping struct {
	match  func(err error) (map[string]string, bool)
	code   codes.Code
	reason string
}

// matchAs is a helper that creates a match function using errors.As,
// extracting metadata via the provided function.
func matchAs[T error](metadataFn func(T) map[string]string) func(error) (map[string]string, bool) {
	return func(err error) (map[string]string, bool) {
		var target T
		if errors.As(err, &target) {
			return metadataFn(target), true
		}

		return nil, false
	}
}

// matchIs is a helper that creates a match function using errors.Is (no metadata).
func matchIs(sentinel error) func(error) (map[string]string, bool) {
	return func(err error) (map[string]string, bool) {
		if errors.Is(err, sentinel) {
			return nil, true
		}

		return nil, false
	}
}

var errorMappings = []errorMapping{
	{matchAs(func(e *domain.ErrLedgerAlreadyExists) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.AlreadyExists, domain.ErrReasonLedgerAlreadyExists},

	{matchAs(func(e *domain.ErrLedgerNotFound) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.NotFound, domain.ErrReasonLedgerNotFound},

	{matchAs(func(e *domain.ErrLedgerDeleted) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.FailedPrecondition, domain.ErrReasonLedgerDeleted},

	{matchAs(func(e *domain.ErrLedgerInMirrorMode) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.FailedPrecondition, domain.ErrReasonLedgerInMirrorMode},

	{matchAs(func(e *domain.ErrLedgerNotInMirrorMode) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.FailedPrecondition, domain.ErrReasonLedgerNotInMirrorMode},

	{matchAs(func(e *domain.ErrIdempotencyKeyConflict) map[string]string {
		return map[string]string{"key": e.Key}
	}), codes.AlreadyExists, domain.ErrReasonIdempotencyKeyConflict},

	{matchAs(func(e *domain.ErrTransactionReferenceConflict) map[string]string {
		return map[string]string{"ledger": e.Ledger, "reference": e.Reference}
	}), codes.AlreadyExists, domain.ErrReasonTransactionReferenceConflict},

	{matchAs(func(e *domain.ErrTransactionNotFound) map[string]string {
		return map[string]string{"transactionId": strconv.FormatUint(e.TransactionID, 10)}
	}), codes.NotFound, domain.ErrReasonTransactionNotFound},

	{matchAs(func(e *domain.ErrTransactionAlreadyReverted) map[string]string {
		return map[string]string{"transactionId": strconv.FormatUint(e.TransactionID, 10)}
	}), codes.FailedPrecondition, domain.ErrReasonTransactionAlreadyReverted},

	{matchAs(func(e *domain.ErrInsufficientFunds) map[string]string {
		return map[string]string{"account": e.Account, "asset": e.Asset, "amount": e.Amount, "balance": e.Balance}
	}), codes.FailedPrecondition, domain.ErrReasonInsufficientFunds},

	{matchAs(func(e *domain.ErrBalanceNotFound) map[string]string {
		return map[string]string{"account": e.Account, "asset": e.Asset}
	}), codes.FailedPrecondition, domain.ErrReasonBalanceNotFound},

	{matchAs(func(e *domain.ErrBalanceNotPreloaded) map[string]string {
		return map[string]string{"account": e.Account, "asset": e.Asset}
	}), codes.FailedPrecondition, domain.ErrReasonBalanceNotPreloaded},

	{matchAs(func(e *domain.ErrNumscriptParse) map[string]string {
		return map[string]string{"details": e.Details}
	}), codes.InvalidArgument, domain.ErrReasonNumscriptParseError},

	{matchAs(func(e *domain.ErrNumscriptNotFound) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.NotFound, domain.ErrReasonNumscriptNotFound},

	{matchAs(func(e *domain.ErrNumscriptVersionAlreadyExists) map[string]string {
		return map[string]string{"name": e.Name, "version": e.Version}
	}), codes.AlreadyExists, domain.ErrReasonNumscriptVersionAlreadyExists},

	{matchAs(func(e *domain.ErrNumscriptInvalidVersion) map[string]string {
		return map[string]string{"version": e.Version}
	}), codes.InvalidArgument, domain.ErrReasonNumscriptInvalidVersion},

	{matchAs(func(e *domain.ErrSinkAlreadyExists) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.AlreadyExists, domain.ErrReasonSinkAlreadyExists},

	{matchAs(func(e *domain.ErrSinkNotFound) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.NotFound, domain.ErrReasonSinkNotFound},

	{matchAs(func(e *domain.ErrPreparedQueryAlreadyExists) map[string]string {
		return map[string]string{"ledger": e.Ledger, "name": e.Name}
	}), codes.AlreadyExists, domain.ErrReasonPreparedQueryAlreadyExists},

	{matchAs(func(e *domain.ErrPreparedQueryNotFound) map[string]string {
		return map[string]string{"ledger": e.Ledger, "name": e.Name}
	}), codes.NotFound, domain.ErrReasonPreparedQueryNotFound},

	{matchAs(func(e *domain.ErrMetadataNotFound) map[string]string {
		return map[string]string{"target": e.Target, "key": e.Key}
	}), codes.NotFound, domain.ErrReasonMetadataNotFound},

	{matchIs(domain.ErrNoPeriodOpen), codes.FailedPrecondition, domain.ErrReasonNoPeriodOpen},

	{matchAs(func(e *domain.ErrPeriodNotFound) map[string]string {
		return map[string]string{"periodId": strconv.FormatUint(e.PeriodID, 10)}
	}), codes.NotFound, domain.ErrReasonPeriodNotFound},

	{matchAs(func(e *domain.ErrPeriodNotClosing) map[string]string {
		return map[string]string{"periodId": strconv.FormatUint(e.PeriodID, 10)}
	}), codes.FailedPrecondition, domain.ErrReasonPeriodNotClosing},

	{matchAs(func(e *domain.ErrInvalidReceipt) map[string]string {
		return map[string]string{"reason": e.Reason}
	}), codes.InvalidArgument, domain.ErrReasonInvalidReceipt},

	// Validation sentinel errors (no metadata)
	{matchIs(domain.ErrTargetRequired), codes.InvalidArgument, domain.ErrReasonValidation},
	{matchIs(domain.ErrMetadataKeyRequired), codes.InvalidArgument, domain.ErrReasonValidation},
	{matchIs(domain.ErrScriptRequired), codes.InvalidArgument, domain.ErrReasonValidation},
	{matchIs(admission.ErrIdempotencyKeyTooLong), codes.InvalidArgument, domain.ErrReasonValidation},
	{matchIs(domain.ErrNumscriptNameRequired), codes.InvalidArgument, domain.ErrReasonValidation},
	{matchIs(domain.ErrNumscriptContentRequired), codes.InvalidArgument, domain.ErrReasonValidation},
	{matchIs(domain.ErrScriptAndReferenceConflict), codes.InvalidArgument, domain.ErrReasonValidation},
	{matchIs(numscript.ErrMetaNotSupported), codes.InvalidArgument, domain.ErrReasonValidation},

	{matchIs(domain.ErrMaintenanceMode), codes.Unavailable, domain.ErrReasonMaintenanceMode},
	{matchIs(domain.ErrStaleProposal), codes.Unavailable, domain.ErrReasonStaleProposal},

	{matchAs(func(e *domain.ErrInvalidCronExpression) map[string]string {
		return map[string]string{"expression": e.Expression, "details": e.Details}
	}), codes.InvalidArgument, domain.ErrReasonInvalidCronExpression},

	{matchAs(func(e *domain.ErrIndexNotFound) map[string]string {
		return map[string]string{"index": e.Index}
	}), codes.FailedPrecondition, domain.ErrReasonIndexNotFound},

	{matchAs(func(e *domain.ErrIndexBuilding) map[string]string {
		return map[string]string{"index": e.Index}
	}), codes.FailedPrecondition, domain.ErrReasonIndexBuilding},

	{matchAs(func(e *domain.ErrAccountNotMatchingType) map[string]string {
		return map[string]string{"address": e.Address}
	}), codes.FailedPrecondition, domain.ErrReasonAccountNotMatchingType},

	{matchAs(func(e *domain.ErrAccountTypeNotFound) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.NotFound, domain.ErrReasonAccountTypeNotFound},

	{matchAs(func(e *domain.ErrAccountTypeAlreadyExists) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.AlreadyExists, domain.ErrReasonAccountTypeAlreadyExists},

	{matchAs(func(e *domain.ErrInvalidPattern) map[string]string {
		return map[string]string{"pattern": e.Pattern, "details": e.Details}
	}), codes.InvalidArgument, domain.ErrReasonInvalidPattern},

	{matchAs(func(e *domain.ErrAccountTypeHasAccounts) map[string]string {
		return map[string]string{"name": e.Name}
	}), codes.FailedPrecondition, domain.ErrReasonAccountTypeHasAccounts},

	{matchAs(func(e *domain.ErrTransientAccountNonZero) map[string]string {
		return map[string]string{"account": e.Account, "asset": e.Asset}
	}), codes.FailedPrecondition, domain.ErrReasonTransientAccountNonZero},
}

// businessErrorToGRPCStatus converts a BusinessError to a gRPC status with ErrorInfo details.
func businessErrorToGRPCStatus(bizErr *domain.BusinessError) *status.Status {
	inner := bizErr.Err

	for _, m := range errorMappings {
		metadata, matched := m.match(inner)
		if !matched {
			continue
		}

		st := status.New(m.code, inner.Error())

		detailed, err := st.WithDetails(&errdetails.ErrorInfo{
			Reason:   m.reason,
			Domain:   errorDomain,
			Metadata: metadata,
		})
		if err != nil {
			return st
		}

		return detailed
	}

	// Unknown business error — fall back to Internal
	return status.New(codes.Internal, inner.Error())
}
