package application

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const errorDomain = "ledger"

// businessErrorToGRPCStatus converts a BusinessError to a gRPC status with ErrorInfo details.
func businessErrorToGRPCStatus(bizErr *processing.BusinessError) *status.Status {
	var (
		code     codes.Code
		reason   string
		metadata map[string]string
	)

	inner := bizErr.Err

	var (
		ledgerAlreadyExists          *processing.ErrLedgerAlreadyExists
		ledgerNotFound               *processing.ErrLedgerNotFound
		idempotencyKeyConflict       *processing.ErrIdempotencyKeyConflict
		transactionReferenceConflict *processing.ErrTransactionReferenceConflict
		transactionNotFound          *processing.ErrTransactionNotFound
		transactionAlreadyReverted   *processing.ErrTransactionAlreadyReverted
		insufficientFunds            *processing.ErrInsufficientFunds
		balanceNotFound              *processing.ErrBalanceNotFound
		balanceNotPreloaded          *processing.ErrBalanceNotPreloaded
		numscriptParse               *processing.ErrNumscriptParse
	)

	switch {
	case errors.As(inner, &ledgerAlreadyExists):
		code = codes.AlreadyExists
		reason = processing.ErrReasonLedgerAlreadyExists
		metadata = map[string]string{"name": ledgerAlreadyExists.Name}

	case errors.As(inner, &ledgerNotFound):
		code = codes.NotFound
		reason = processing.ErrReasonLedgerNotFound
		metadata = map[string]string{"name": ledgerNotFound.Name}

	case errors.As(inner, &idempotencyKeyConflict):
		code = codes.AlreadyExists
		reason = processing.ErrReasonIdempotencyKeyConflict
		metadata = map[string]string{"key": idempotencyKeyConflict.Key}

	case errors.As(inner, &transactionReferenceConflict):
		code = codes.AlreadyExists
		reason = processing.ErrReasonTransactionReferenceConflict
		metadata = map[string]string{
			"ledgerId":  fmt.Sprintf("%d", transactionReferenceConflict.LedgerID),
			"reference": transactionReferenceConflict.Reference,
		}

	case errors.As(inner, &transactionNotFound):
		code = codes.NotFound
		reason = processing.ErrReasonTransactionNotFound
		metadata = map[string]string{
			"transactionId": fmt.Sprintf("%d", transactionNotFound.TransactionID),
		}

	case errors.As(inner, &transactionAlreadyReverted):
		code = codes.FailedPrecondition
		reason = processing.ErrReasonTransactionAlreadyReverted
		metadata = map[string]string{
			"transactionId": fmt.Sprintf("%d", transactionAlreadyReverted.TransactionID),
		}

	case errors.As(inner, &insufficientFunds):
		code = codes.FailedPrecondition
		reason = processing.ErrReasonInsufficientFunds
		metadata = map[string]string{
			"account": insufficientFunds.Account,
			"asset":   insufficientFunds.Asset,
			"amount":  insufficientFunds.Amount,
			"balance": insufficientFunds.Balance,
		}

	case errors.As(inner, &balanceNotFound):
		code = codes.FailedPrecondition
		reason = processing.ErrReasonBalanceNotFound
		metadata = map[string]string{
			"account": balanceNotFound.Account,
			"asset":   balanceNotFound.Asset,
		}

	case errors.As(inner, &balanceNotPreloaded):
		code = codes.FailedPrecondition
		reason = processing.ErrReasonBalanceNotPreloaded
		metadata = map[string]string{
			"account": balanceNotPreloaded.Account,
			"asset":   balanceNotPreloaded.Asset,
		}

	case errors.As(inner, &numscriptParse):
		code = codes.InvalidArgument
		reason = processing.ErrReasonNumscriptParseError
		metadata = map[string]string{"details": numscriptParse.Details}

	case errors.Is(inner, processing.ErrTargetRequired),
		errors.Is(inner, processing.ErrMetadataKeyRequired),
		errors.Is(inner, processing.ErrScriptRequired):
		code = codes.InvalidArgument
		reason = processing.ErrReasonValidation

	default:
		// Unknown business error — fall back to Internal
		return status.New(codes.Internal, inner.Error())
	}

	st := status.New(code, inner.Error())
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   reason,
		Domain:   errorDomain,
		Metadata: metadata,
	})
	if err != nil {
		// If attaching details fails, return the plain status
		return st
	}

	return detailed
}
