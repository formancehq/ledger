package grpc

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/application/admission"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const errorDomain = "ledger"

// businessErrorToGRPCStatus converts a BusinessError to a gRPC status with ErrorInfo details.
func businessErrorToGRPCStatus(bizErr *domain.BusinessError) *status.Status {
	var (
		code     codes.Code
		reason   string
		metadata map[string]string
	)

	inner := bizErr.Err

	var (
		ledgerAlreadyExists           *domain.ErrLedgerAlreadyExists
		ledgerNotFound                *domain.ErrLedgerNotFound
		ledgerInMirrorMode            *domain.ErrLedgerInMirrorMode
		ledgerNotInMirrorMode         *domain.ErrLedgerNotInMirrorMode
		idempotencyKeyConflict        *domain.ErrIdempotencyKeyConflict
		transactionReferenceConflict  *domain.ErrTransactionReferenceConflict
		transactionNotFound           *domain.ErrTransactionNotFound
		transactionAlreadyReverted    *domain.ErrTransactionAlreadyReverted
		insufficientFunds             *domain.ErrInsufficientFunds
		balanceNotFound               *domain.ErrBalanceNotFound
		balanceNotPreloaded           *numscript.ErrBalanceNotPreloaded
		numscriptParse                *numscript.ErrNumscriptParse
		numscriptNotFound             *domain.ErrNumscriptNotFound
		numscriptVersionAlreadyExists *domain.ErrNumscriptVersionAlreadyExists
		numscriptInvalidVersion       *domain.ErrNumscriptInvalidVersion
		sinkAlreadyExists             *domain.ErrSinkAlreadyExists
		sinkNotFound                  *domain.ErrSinkNotFound
		metadataNotFound              *domain.ErrMetadataNotFound
		preparedQueryAlreadyExists    *domain.ErrPreparedQueryAlreadyExists
		preparedQueryNotFound         *domain.ErrPreparedQueryNotFound
		periodNotFound                *domain.ErrPeriodNotFound
		periodNotClosing              *domain.ErrPeriodNotClosing
		invalidReceipt                *domain.ErrInvalidReceipt
		invalidCronExpression         *domain.ErrInvalidCronExpression
		indexNotFound                 *domain.ErrIndexNotFound
		indexBuilding                 *domain.ErrIndexBuilding
		accountNotInChart             *domain.ErrAccountNotInChart
		invalidChart                  *domain.ErrInvalidChart
	)

	switch {
	case errors.As(inner, &ledgerAlreadyExists):
		code = codes.AlreadyExists
		reason = domain.ErrReasonLedgerAlreadyExists
		metadata = map[string]string{"name": ledgerAlreadyExists.Name}

	case errors.As(inner, &ledgerNotFound):
		code = codes.NotFound
		reason = domain.ErrReasonLedgerNotFound
		metadata = map[string]string{"name": ledgerNotFound.Name}

	case errors.As(inner, &ledgerInMirrorMode):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonLedgerInMirrorMode
		metadata = map[string]string{"name": ledgerInMirrorMode.Name}

	case errors.As(inner, &ledgerNotInMirrorMode):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonLedgerNotInMirrorMode
		metadata = map[string]string{"name": ledgerNotInMirrorMode.Name}

	case errors.As(inner, &idempotencyKeyConflict):
		code = codes.AlreadyExists
		reason = domain.ErrReasonIdempotencyKeyConflict
		metadata = map[string]string{"key": idempotencyKeyConflict.Key}

	case errors.As(inner, &transactionReferenceConflict):
		code = codes.AlreadyExists
		reason = domain.ErrReasonTransactionReferenceConflict
		metadata = map[string]string{
			"ledger":    transactionReferenceConflict.Ledger,
			"reference": transactionReferenceConflict.Reference,
		}

	case errors.As(inner, &transactionNotFound):
		code = codes.NotFound
		reason = domain.ErrReasonTransactionNotFound
		metadata = map[string]string{
			"transactionId": fmt.Sprintf("%d", transactionNotFound.TransactionID),
		}

	case errors.As(inner, &transactionAlreadyReverted):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonTransactionAlreadyReverted
		metadata = map[string]string{
			"transactionId": fmt.Sprintf("%d", transactionAlreadyReverted.TransactionID),
		}

	case errors.As(inner, &insufficientFunds):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonInsufficientFunds
		metadata = map[string]string{
			"account": insufficientFunds.Account,
			"asset":   insufficientFunds.Asset,
			"amount":  insufficientFunds.Amount,
			"balance": insufficientFunds.Balance,
		}

	case errors.As(inner, &balanceNotFound):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonBalanceNotFound
		metadata = map[string]string{
			"account": balanceNotFound.Account,
			"asset":   balanceNotFound.Asset,
		}

	case errors.As(inner, &balanceNotPreloaded):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonBalanceNotPreloaded
		metadata = map[string]string{
			"account": balanceNotPreloaded.Account,
			"asset":   balanceNotPreloaded.Asset,
		}

	case errors.As(inner, &numscriptParse):
		code = codes.InvalidArgument
		reason = domain.ErrReasonNumscriptParseError
		metadata = map[string]string{"details": numscriptParse.Details}

	case errors.As(inner, &numscriptNotFound):
		code = codes.NotFound
		reason = domain.ErrReasonNumscriptNotFound
		metadata = map[string]string{"name": numscriptNotFound.Name}

	case errors.As(inner, &numscriptVersionAlreadyExists):
		code = codes.AlreadyExists
		reason = domain.ErrReasonNumscriptVersionAlreadyExists
		metadata = map[string]string{"name": numscriptVersionAlreadyExists.Name, "version": numscriptVersionAlreadyExists.Version}

	case errors.As(inner, &numscriptInvalidVersion):
		code = codes.InvalidArgument
		reason = domain.ErrReasonNumscriptInvalidVersion
		metadata = map[string]string{"version": numscriptInvalidVersion.Version}

	case errors.As(inner, &sinkAlreadyExists):
		code = codes.AlreadyExists
		reason = domain.ErrReasonSinkAlreadyExists
		metadata = map[string]string{"name": sinkAlreadyExists.Name}

	case errors.As(inner, &sinkNotFound):
		code = codes.NotFound
		reason = domain.ErrReasonSinkNotFound
		metadata = map[string]string{"name": sinkNotFound.Name}

	case errors.As(inner, &preparedQueryAlreadyExists):
		code = codes.AlreadyExists
		reason = domain.ErrReasonPreparedQueryAlreadyExists
		metadata = map[string]string{
			"ledger": preparedQueryAlreadyExists.Ledger,
			"name":   preparedQueryAlreadyExists.Name,
		}

	case errors.As(inner, &preparedQueryNotFound):
		code = codes.NotFound
		reason = domain.ErrReasonPreparedQueryNotFound
		metadata = map[string]string{
			"ledger": preparedQueryNotFound.Ledger,
			"name":   preparedQueryNotFound.Name,
		}

	case errors.As(inner, &metadataNotFound):
		code = codes.NotFound
		reason = domain.ErrReasonMetadataNotFound
		metadata = map[string]string{"target": metadataNotFound.Target, "key": metadataNotFound.Key}

	case errors.Is(inner, domain.ErrNoPeriodOpen):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonNoPeriodOpen

	case errors.Is(inner, domain.ErrPeriodAlreadyClosing):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonPeriodAlreadyClosing

	case errors.As(inner, &periodNotFound):
		code = codes.NotFound
		reason = domain.ErrReasonPeriodNotFound
		metadata = map[string]string{
			"periodId": fmt.Sprintf("%d", periodNotFound.PeriodID),
		}

	case errors.As(inner, &periodNotClosing):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonPeriodNotClosing
		metadata = map[string]string{
			"periodId": fmt.Sprintf("%d", periodNotClosing.PeriodID),
		}

	case errors.As(inner, &invalidReceipt):
		code = codes.InvalidArgument
		reason = domain.ErrReasonInvalidReceipt
		metadata = map[string]string{
			"reason": invalidReceipt.Reason,
		}

	case errors.Is(inner, domain.ErrTargetRequired),
		errors.Is(inner, domain.ErrMetadataKeyRequired),
		errors.Is(inner, numscript.ErrScriptRequired),
		errors.Is(inner, admission.ErrIdempotencyKeyTooLong),
		errors.Is(inner, domain.ErrNumscriptNameRequired),
		errors.Is(inner, domain.ErrNumscriptContentRequired),
		errors.Is(inner, domain.ErrScriptAndReferenceConflict):
		code = codes.InvalidArgument
		reason = domain.ErrReasonValidation

	case errors.Is(inner, domain.ErrMaintenanceMode):
		code = codes.Unavailable
		reason = domain.ErrReasonMaintenanceMode

	case errors.As(inner, &invalidCronExpression):
		code = codes.InvalidArgument
		reason = domain.ErrReasonInvalidCronExpression
		metadata = map[string]string{
			"expression": invalidCronExpression.Expression,
			"details":    invalidCronExpression.Details,
		}

	case errors.As(inner, &indexNotFound):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonIndexNotFound
		metadata = map[string]string{"index": indexNotFound.Index}

	case errors.As(inner, &indexBuilding):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonIndexBuilding
		metadata = map[string]string{"index": indexBuilding.Index}

	case errors.As(inner, &accountNotInChart):
		code = codes.FailedPrecondition
		reason = domain.ErrReasonAccountNotInChart
		metadata = map[string]string{"address": accountNotInChart.Address}

	case errors.As(inner, &invalidChart):
		code = codes.InvalidArgument
		reason = domain.ErrReasonInvalidChart
		metadata = map[string]string{"details": invalidChart.Details}

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
