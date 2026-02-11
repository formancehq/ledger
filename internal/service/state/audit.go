package state

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
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
		balanceNotPreloaded          *processing.ErrBalanceNotPreloaded
		numscriptParse               *processing.ErrNumscriptParse
		nonDeterministic             *processing.ErrNonDeterministicScript
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
		failure.Context["ledgerId"] = fmt.Sprintf("%d", transactionReferenceConflict.LedgerID)
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
		failure.Context["amount"] = insufficientFunds.Amount.String()
		failure.Context["balance"] = insufficientFunds.Balance.String()

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
		errors.Is(err, processing.ErrScriptRequired):
		failure.ErrorType = processing.ErrReasonValidation

	default:
		failure.ErrorType = "UNKNOWN"
	}

	return failure
}

// extractLogSequences extracts the sequence numbers from a slice of logs.
func extractLogSequences(logs []*commonpb.Log) []uint64 {
	sequences := make([]uint64, len(logs))
	for i, log := range logs {
		sequences[i] = log.Sequence
	}
	return sequences
}
