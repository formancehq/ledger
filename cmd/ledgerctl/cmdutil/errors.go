package cmdutil

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing/numscript"
	"github.com/pterm/pterm"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// FormatGRPCError returns a clean error for gRPC errors suitable for CLI display.
// For business errors, it reconstructs the typed error and uses its message.
// For other gRPC errors, it uses the status message (without gRPC framing).
// For non-gRPC errors, it wraps the original error.
func FormatGRPCError(context string, err error) error {
	if bizErr := BusinessErrorFromGRPC(err); bizErr != nil {
		printErrorDetails(bizErr.Err)
		return fmt.Errorf("%s: %s", context, bizErr.Err.Error())
	}
	if st, ok := status.FromError(err); ok {
		return fmt.Errorf("%s: %s", context, st.Message())
	}
	return fmt.Errorf("%s: %w", context, err)
}

// printErrorDetails prints structured details for specific business error types.
func printErrorDetails(err error) {
	var (
		insufficient *processing.ErrInsufficientFunds
		refConflict  *processing.ErrTransactionReferenceConflict
	)

	switch {
	case errors.As(err, &insufficient):
		pterm.Println()
		pterm.Printf("  Account: %s\n", pterm.Cyan(insufficient.Account))
		pterm.Printf("  Asset:   %s\n", pterm.Yellow(insufficient.Asset))
		pterm.Printf("  Balance: %s\n", pterm.Red(insufficient.Balance))
		pterm.Printf("  Needed:  %s\n", insufficient.Amount)
	case errors.As(err, &refConflict):
		pterm.Println()
		pterm.Printf("  Reference: %s\n", pterm.Cyan(refConflict.Reference))
	}
}

// BusinessErrorFromGRPC extracts a BusinessError from a gRPC status error.
// Returns nil if the error is not a business error (no ErrorInfo with domain "ledger").
func BusinessErrorFromGRPC(err error) *processing.BusinessError {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}

	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.Domain != "ledger" {
			continue
		}

		inner := reconstructError(info.Reason, info.Metadata, st.Message())
		if inner != nil {
			return &processing.BusinessError{Err: inner}
		}
	}

	return nil
}

// reconstructError rebuilds the typed error from the reason and metadata.
func reconstructError(reason string, metadata map[string]string, message string) error {
	switch reason {
	case processing.ErrReasonLedgerAlreadyExists:
		return &processing.ErrLedgerAlreadyExists{Name: metadata["name"]}

	case processing.ErrReasonLedgerNotFound:
		return &processing.ErrLedgerNotFound{Name: metadata["name"]}

	case processing.ErrReasonIdempotencyKeyConflict:
		return &processing.ErrIdempotencyKeyConflict{Key: metadata["key"]}

	case processing.ErrReasonTransactionReferenceConflict:
		ledgerID, _ := strconv.ParseUint(metadata["ledgerId"], 10, 32)
		return &processing.ErrTransactionReferenceConflict{
			LedgerID:  uint32(ledgerID),
			Reference: metadata["reference"],
		}

	case processing.ErrReasonTransactionNotFound:
		txID, _ := strconv.ParseUint(metadata["transactionId"], 10, 64)
		return &processing.ErrTransactionNotFound{TransactionID: txID}

	case processing.ErrReasonTransactionAlreadyReverted:
		txID, _ := strconv.ParseUint(metadata["transactionId"], 10, 64)
		return &processing.ErrTransactionAlreadyReverted{TransactionID: txID}

	case processing.ErrReasonInsufficientFunds:
		return &processing.ErrInsufficientFunds{
			Account: metadata["account"],
			Asset:   metadata["asset"],
			Amount:  metadata["amount"],
			Balance: metadata["balance"],
		}

	case processing.ErrReasonBalanceNotFound:
		return &processing.ErrBalanceNotFound{
			Account: metadata["account"],
			Asset:   metadata["asset"],
		}

	case processing.ErrReasonBalanceNotPreloaded:
		return &numscript.ErrBalanceNotPreloaded{
			Account: metadata["account"],
			Asset:   metadata["asset"],
		}

	case processing.ErrReasonNumscriptParseError:
		return &numscript.ErrNumscriptParse{Details: metadata["details"]}

	case processing.ErrReasonValidation:
		return fmt.Errorf("%s", message)

	default:
		return nil
	}
}
