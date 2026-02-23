package application

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/service/admission"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing/numscript"
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
		balanceNotPreloaded          *numscript.ErrBalanceNotPreloaded
		numscriptParse               *numscript.ErrNumscriptParse
		sinkAlreadyExists            *processing.ErrSinkAlreadyExists
		sinkNotFound                 *processing.ErrSinkNotFound
		metadataNotFound             *processing.ErrMetadataNotFound
		periodNotFound               *processing.ErrPeriodNotFound
		periodNotClosing             *processing.ErrPeriodNotClosing
		invalidReceipt               *processing.ErrInvalidReceipt
		invalidCronExpression        *processing.ErrInvalidCronExpression
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

	case errors.As(inner, &sinkAlreadyExists):
		code = codes.AlreadyExists
		reason = processing.ErrReasonSinkAlreadyExists
		metadata = map[string]string{"name": sinkAlreadyExists.Name}

	case errors.As(inner, &sinkNotFound):
		code = codes.NotFound
		reason = processing.ErrReasonSinkNotFound
		metadata = map[string]string{"name": sinkNotFound.Name}

	case errors.As(inner, &metadataNotFound):
		code = codes.NotFound
		reason = processing.ErrReasonMetadataNotFound
		metadata = map[string]string{"target": metadataNotFound.Target, "key": metadataNotFound.Key}

	case errors.Is(inner, processing.ErrNoPeriodOpen):
		code = codes.FailedPrecondition
		reason = processing.ErrReasonNoPeriodOpen

	case errors.Is(inner, processing.ErrPeriodAlreadyClosing):
		code = codes.FailedPrecondition
		reason = processing.ErrReasonPeriodAlreadyClosing

	case errors.As(inner, &periodNotFound):
		code = codes.NotFound
		reason = processing.ErrReasonPeriodNotFound
		metadata = map[string]string{
			"periodId": fmt.Sprintf("%d", periodNotFound.PeriodID),
		}

	case errors.As(inner, &periodNotClosing):
		code = codes.FailedPrecondition
		reason = processing.ErrReasonPeriodNotClosing
		metadata = map[string]string{
			"periodId": fmt.Sprintf("%d", periodNotClosing.PeriodID),
		}

	case errors.As(inner, &invalidReceipt):
		code = codes.InvalidArgument
		reason = processing.ErrReasonInvalidReceipt
		metadata = map[string]string{
			"reason": invalidReceipt.Reason,
		}

	case errors.Is(inner, processing.ErrTargetRequired),
		errors.Is(inner, processing.ErrMetadataKeyRequired),
		errors.Is(inner, numscript.ErrScriptRequired),
		errors.Is(inner, admission.ErrIdempotencyKeyTooLong):
		code = codes.InvalidArgument
		reason = processing.ErrReasonValidation

	case errors.Is(inner, processing.ErrMaintenanceMode):
		code = codes.Unavailable
		reason = processing.ErrReasonMaintenanceMode

	case errors.As(inner, &invalidCronExpression):
		code = codes.InvalidArgument
		reason = processing.ErrReasonInvalidCronExpression
		metadata = map[string]string{
			"expression": invalidCronExpression.Expression,
			"details":    invalidCronExpression.Details,
		}

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
