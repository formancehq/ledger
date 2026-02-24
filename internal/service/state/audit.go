package state

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing/numscript"
)

// buildAuditFailure extracts the error type and context from a processing error
// to build an AuditFailure proto message.
func buildAuditFailure(err error) *auditpb.AuditFailure {
	failure := &auditpb.AuditFailure{
		Message: err.Error(),
		Context: make(map[string]string),
	}

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
		nonDeterministic             *numscript.ErrNonDeterministicScript
	)

	switch {
	case errors.As(err, &ledgerAlreadyExists):
		failure.ErrorType = processing.ErrReasonLedgerAlreadyExists
		failure.Context["name"] = ledgerAlreadyExists.Name

	case errors.As(err, &ledgerNotFound):
		failure.ErrorType = processing.ErrReasonLedgerNotFound
		failure.Context["name"] = ledgerNotFound.Name

	case errors.As(err, &idempotencyKeyConflict):
		failure.ErrorType = processing.ErrReasonIdempotencyKeyConflict
		failure.Context["key"] = idempotencyKeyConflict.Key

	case errors.As(err, &transactionReferenceConflict):
		failure.ErrorType = processing.ErrReasonTransactionReferenceConflict
		failure.Context["ledger"] = transactionReferenceConflict.Ledger
		failure.Context["reference"] = transactionReferenceConflict.Reference

	case errors.As(err, &transactionNotFound):
		failure.ErrorType = processing.ErrReasonTransactionNotFound
		failure.Context["transactionId"] = fmt.Sprintf("%d", transactionNotFound.TransactionID)

	case errors.As(err, &transactionAlreadyReverted):
		failure.ErrorType = processing.ErrReasonTransactionAlreadyReverted
		failure.Context["transactionId"] = fmt.Sprintf("%d", transactionAlreadyReverted.TransactionID)

	case errors.As(err, &insufficientFunds):
		failure.ErrorType = processing.ErrReasonInsufficientFunds
		failure.Context["account"] = insufficientFunds.Account
		failure.Context["asset"] = insufficientFunds.Asset
		failure.Context["amount"] = insufficientFunds.Amount
		failure.Context["balance"] = insufficientFunds.Balance

	case errors.As(err, &balanceNotFound):
		failure.ErrorType = processing.ErrReasonBalanceNotFound
		failure.Context["account"] = balanceNotFound.Account
		failure.Context["asset"] = balanceNotFound.Asset

	case errors.As(err, &balanceNotPreloaded):
		failure.ErrorType = processing.ErrReasonBalanceNotPreloaded
		failure.Context["account"] = balanceNotPreloaded.Account
		failure.Context["asset"] = balanceNotPreloaded.Asset

	case errors.As(err, &numscriptParse):
		failure.ErrorType = processing.ErrReasonNumscriptParseError
		failure.Context["details"] = numscriptParse.Details

	case errors.As(err, &nonDeterministic):
		failure.ErrorType = "NON_DETERMINISTIC_SCRIPT"
		failure.Context["method"] = nonDeterministic.Method

	case errors.Is(err, processing.ErrTargetRequired),
		errors.Is(err, processing.ErrMetadataKeyRequired),
		errors.Is(err, numscript.ErrScriptRequired):
		failure.ErrorType = processing.ErrReasonValidation

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
