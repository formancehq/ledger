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

// buildAuditItems creates AuditItem entries from orders and their associated logs.
// For failure cases (logs is nil), all items get LogSequence=0.
func buildAuditItems(orders []*raftcmdpb.Order, logs []*raftcmdpb.CreatedLogOrReference) []*auditpb.AuditItem {
	items := make([]*auditpb.AuditItem, len(orders))

	for i, order := range orders {
		item := &auditpb.AuditItem{
			OrderIndex: uint32(i),
			Order:      order,
		}

		if i < len(logs) {
			if created := logs[i].GetCreatedLog(); created != nil {
				item.LogSequence = created.GetSequence()
			} else if refSeq := logs[i].GetReferenceSequence(); refSeq > 0 {
				item.LogSequence = refSeq
			}
		}

		items[i] = item
	}

	return items
}

// extractLedgers returns the distinct ledger names targeted by a set of orders.
func extractLedgers(orders []*raftcmdpb.Order) []string {
	seen := make(map[string]struct{})

	for _, order := range orders {
		var ledger string

		switch {
		case order.GetApply() != nil:
			ledger = order.GetApply().GetLedger()
		case order.GetCreateLedger() != nil:
			ledger = order.GetCreateLedger().GetName()
		case order.GetDeleteLedger() != nil:
			ledger = order.GetDeleteLedger().GetName()
		case order.GetMirrorIngest() != nil:
			ledger = order.GetMirrorIngest().GetLedger()
		case order.GetPromoteLedger() != nil:
			ledger = order.GetPromoteLedger().GetLedger()
		case order.GetSaveLedgerMetadata() != nil:
			ledger = order.GetSaveLedgerMetadata().GetLedger()
		case order.GetDeleteLedgerMetadata() != nil:
			ledger = order.GetDeleteLedgerMetadata().GetLedger()
		}

		if ledger != "" {
			seen[ledger] = struct{}{}
		}
	}

	if len(seen) == 0 {
		return nil
	}

	ledgers := make([]string, 0, len(seen))
	for l := range seen {
		ledgers = append(ledgers, l)
	}

	return ledgers
}

// extractLogSequenceRange returns the min and max log sequence from a slice of
// CreatedLogOrReference. For created logs it uses the log sequence; for
// idempotent references it uses the reference sequence. Returns (0, 0) if empty.
func extractLogSequenceRange(logsOrRefs []*raftcmdpb.CreatedLogOrReference) (minSeq, maxSeq uint64) {
	for _, logOrRef := range logsOrRefs {
		var seq uint64
		if created := logOrRef.GetCreatedLog(); created != nil {
			seq = created.GetSequence()
		} else {
			seq = logOrRef.GetReferenceSequence()
		}

		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}

		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return minSeq, maxSeq
}
