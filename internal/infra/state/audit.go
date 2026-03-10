package state

import (
	"errors"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// buildAuditFailure extracts the error type and context from a processing error
// to build an AuditFailure proto message.
func buildAuditFailure(err error) *auditpb.AuditFailure {
	failure := &auditpb.AuditFailure{
		Message: err.Error(),
		Context: make(map[string]string),
	}

	var (
		ledgerAlreadyExists          *domain.ErrLedgerAlreadyExists
		ledgerNotFound               *domain.ErrLedgerNotFound
		ledgerInMirrorMode           *domain.ErrLedgerInMirrorMode
		ledgerNotInMirrorMode        *domain.ErrLedgerNotInMirrorMode
		idempotencyKeyConflict       *domain.ErrIdempotencyKeyConflict
		transactionReferenceConflict *domain.ErrTransactionReferenceConflict
		transactionNotFound          *domain.ErrTransactionNotFound
		transactionAlreadyReverted   *domain.ErrTransactionAlreadyReverted
		insufficientFunds            *domain.ErrInsufficientFunds
		balanceNotFound              *domain.ErrBalanceNotFound
		balanceNotPreloaded          *domain.ErrBalanceNotPreloaded
		numscriptParse               *domain.ErrNumscriptParse
		nonDeterministic             *numscript.ErrNonDeterministicScript
		sinkAlreadyExists            *domain.ErrSinkAlreadyExists
		sinkNotFound                 *domain.ErrSinkNotFound
		metadataNotFound             *domain.ErrMetadataNotFound
		periodNotFound               *domain.ErrPeriodNotFound
		periodNotClosing             *domain.ErrPeriodNotClosing
		periodNotClosed              *domain.ErrPeriodNotClosed
		periodNotArchiving           *domain.ErrPeriodNotArchiving
		invalidReceipt               *domain.ErrInvalidReceipt
		invalidCronExpression        *domain.ErrInvalidCronExpression
	)

	switch {
	case errors.As(err, &ledgerAlreadyExists):
		failure.ErrorType = domain.ErrReasonLedgerAlreadyExists
		failure.Context["name"] = ledgerAlreadyExists.Name

	case errors.As(err, &ledgerNotFound):
		failure.ErrorType = domain.ErrReasonLedgerNotFound
		failure.Context["name"] = ledgerNotFound.Name

	case errors.As(err, &ledgerInMirrorMode):
		failure.ErrorType = domain.ErrReasonLedgerInMirrorMode
		failure.Context["name"] = ledgerInMirrorMode.Name

	case errors.As(err, &ledgerNotInMirrorMode):
		failure.ErrorType = domain.ErrReasonLedgerNotInMirrorMode
		failure.Context["name"] = ledgerNotInMirrorMode.Name

	case errors.As(err, &idempotencyKeyConflict):
		failure.ErrorType = domain.ErrReasonIdempotencyKeyConflict
		failure.Context["key"] = idempotencyKeyConflict.Key

	case errors.As(err, &transactionReferenceConflict):
		failure.ErrorType = domain.ErrReasonTransactionReferenceConflict
		failure.Context["ledger"] = transactionReferenceConflict.Ledger
		failure.Context["reference"] = transactionReferenceConflict.Reference

	case errors.As(err, &transactionNotFound):
		failure.ErrorType = domain.ErrReasonTransactionNotFound
		failure.Context["transactionId"] = strconv.FormatUint(transactionNotFound.TransactionID, 10)

	case errors.As(err, &transactionAlreadyReverted):
		failure.ErrorType = domain.ErrReasonTransactionAlreadyReverted
		failure.Context["transactionId"] = strconv.FormatUint(transactionAlreadyReverted.TransactionID, 10)

	case errors.As(err, &insufficientFunds):
		failure.ErrorType = domain.ErrReasonInsufficientFunds
		failure.Context["account"] = insufficientFunds.Account
		failure.Context["asset"] = insufficientFunds.Asset
		failure.Context["amount"] = insufficientFunds.Amount
		failure.Context["balance"] = insufficientFunds.Balance

	case errors.As(err, &balanceNotFound):
		failure.ErrorType = domain.ErrReasonBalanceNotFound
		failure.Context["account"] = balanceNotFound.Account
		failure.Context["asset"] = balanceNotFound.Asset

	case errors.As(err, &balanceNotPreloaded):
		failure.ErrorType = domain.ErrReasonBalanceNotPreloaded
		failure.Context["account"] = balanceNotPreloaded.Account
		failure.Context["asset"] = balanceNotPreloaded.Asset

	case errors.As(err, &numscriptParse):
		failure.ErrorType = domain.ErrReasonNumscriptParseError
		failure.Context["details"] = numscriptParse.Details

	case errors.As(err, &nonDeterministic):
		failure.ErrorType = "NON_DETERMINISTIC_SCRIPT"
		failure.Context["method"] = nonDeterministic.Method

	case errors.As(err, &sinkAlreadyExists):
		failure.ErrorType = domain.ErrReasonSinkAlreadyExists
		failure.Context["name"] = sinkAlreadyExists.Name

	case errors.As(err, &sinkNotFound):
		failure.ErrorType = domain.ErrReasonSinkNotFound
		failure.Context["name"] = sinkNotFound.Name

	case errors.As(err, &metadataNotFound):
		failure.ErrorType = domain.ErrReasonMetadataNotFound
		failure.Context["target"] = metadataNotFound.Target
		failure.Context["key"] = metadataNotFound.Key

	case errors.Is(err, domain.ErrNoPeriodOpen):
		failure.ErrorType = domain.ErrReasonNoPeriodOpen

	case errors.Is(err, domain.ErrPeriodAlreadyClosing):
		failure.ErrorType = domain.ErrReasonPeriodAlreadyClosing

	case errors.As(err, &periodNotFound):
		failure.ErrorType = domain.ErrReasonPeriodNotFound
		failure.Context["periodId"] = strconv.FormatUint(periodNotFound.PeriodID, 10)

	case errors.As(err, &periodNotClosing):
		failure.ErrorType = domain.ErrReasonPeriodNotClosing
		failure.Context["periodId"] = strconv.FormatUint(periodNotClosing.PeriodID, 10)

	case errors.As(err, &periodNotClosed):
		failure.ErrorType = domain.ErrReasonPeriodNotClosed
		failure.Context["periodId"] = strconv.FormatUint(periodNotClosed.PeriodID, 10)

	case errors.As(err, &periodNotArchiving):
		failure.ErrorType = domain.ErrReasonPeriodNotArchiving
		failure.Context["periodId"] = strconv.FormatUint(periodNotArchiving.PeriodID, 10)

	case errors.As(err, &invalidReceipt):
		failure.ErrorType = domain.ErrReasonInvalidReceipt
		failure.Context["reason"] = invalidReceipt.Reason

	case errors.As(err, &invalidCronExpression):
		failure.ErrorType = domain.ErrReasonInvalidCronExpression
		failure.Context["expression"] = invalidCronExpression.Expression
		failure.Context["details"] = invalidCronExpression.Details

	case errors.Is(err, domain.ErrMaintenanceMode):
		failure.ErrorType = domain.ErrReasonMaintenanceMode

	case errors.Is(err, domain.ErrTargetRequired),
		errors.Is(err, domain.ErrMetadataKeyRequired),
		errors.Is(err, domain.ErrScriptRequired):
		failure.ErrorType = domain.ErrReasonValidation

	default:
		failure.ErrorType = "UNKNOWN"
	}

	return failure
}

// extractLogSequencesFromLogsOrRefs extracts the sequence numbers from a slice of
// CreatedLogOrReference. For created logs it returns the log sequence; for reference
// sequences it returns the reference directly.
func extractLogSequencesFromLogsOrRefs(logsOrRefs []*raftcmdpb.CreatedLogOrReference) []uint64 {
	sequences := make([]uint64, len(logsOrRefs))
	for i, logOrRef := range logsOrRefs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			sequences[i] = created.GetSequence()
		} else {
			sequences[i] = logOrRef.GetReferenceSequence()
		}
	}

	return sequences
}
