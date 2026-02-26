package state

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
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
		idempotencyKeyConflict       *domain.ErrIdempotencyKeyConflict
		transactionReferenceConflict *domain.ErrTransactionReferenceConflict
		transactionNotFound          *domain.ErrTransactionNotFound
		transactionAlreadyReverted   *domain.ErrTransactionAlreadyReverted
		insufficientFunds            *domain.ErrInsufficientFunds
		balanceNotFound              *domain.ErrBalanceNotFound
		balanceNotPreloaded          *numscript.ErrBalanceNotPreloaded
		numscriptParse               *numscript.ErrNumscriptParse
		nonDeterministic             *numscript.ErrNonDeterministicScript
	)

	switch {
	case errors.As(err, &ledgerAlreadyExists):
		failure.ErrorType = domain.ErrReasonLedgerAlreadyExists
		failure.Context["name"] = ledgerAlreadyExists.Name

	case errors.As(err, &ledgerNotFound):
		failure.ErrorType = domain.ErrReasonLedgerNotFound
		failure.Context["name"] = ledgerNotFound.Name

	case errors.As(err, &idempotencyKeyConflict):
		failure.ErrorType = domain.ErrReasonIdempotencyKeyConflict
		failure.Context["key"] = idempotencyKeyConflict.Key

	case errors.As(err, &transactionReferenceConflict):
		failure.ErrorType = domain.ErrReasonTransactionReferenceConflict
		failure.Context["ledger"] = transactionReferenceConflict.Ledger
		failure.Context["reference"] = transactionReferenceConflict.Reference

	case errors.As(err, &transactionNotFound):
		failure.ErrorType = domain.ErrReasonTransactionNotFound
		failure.Context["transactionId"] = fmt.Sprintf("%d", transactionNotFound.TransactionID)

	case errors.As(err, &transactionAlreadyReverted):
		failure.ErrorType = domain.ErrReasonTransactionAlreadyReverted
		failure.Context["transactionId"] = fmt.Sprintf("%d", transactionAlreadyReverted.TransactionID)

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

	case errors.Is(err, domain.ErrTargetRequired),
		errors.Is(err, domain.ErrMetadataKeyRequired),
		errors.Is(err, numscript.ErrScriptRequired):
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
			sequences[i] = created.Sequence
		} else {
			sequences[i] = logOrRef.GetReferenceSequence()
		}
	}
	return sequences
}
