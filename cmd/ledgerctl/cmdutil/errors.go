package cmdutil

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
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
		insufficient *domain.ErrInsufficientFunds
		refConflict  *domain.ErrTransactionReferenceConflict
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
func BusinessErrorFromGRPC(err error) *domain.BusinessError {
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
			return &domain.BusinessError{Err: inner}
		}
	}

	return nil
}

// reconstructError rebuilds the typed error from the reason and metadata.
func reconstructError(reason string, metadata map[string]string, message string) error {
	switch reason {
	case domain.ErrReasonLedgerAlreadyExists:
		return &domain.ErrLedgerAlreadyExists{Name: metadata["name"]}

	case domain.ErrReasonLedgerNotFound:
		return &domain.ErrLedgerNotFound{Name: metadata["name"]}

	case domain.ErrReasonIdempotencyKeyConflict:
		return &domain.ErrIdempotencyKeyConflict{Key: metadata["key"]}

	case domain.ErrReasonTransactionReferenceConflict:
		return &domain.ErrTransactionReferenceConflict{
			Ledger:    metadata["ledger"],
			Reference: metadata["reference"],
		}

	case domain.ErrReasonTransactionNotFound:
		txID, _ := strconv.ParseUint(metadata["transactionId"], 10, 64)
		return &domain.ErrTransactionNotFound{TransactionID: txID}

	case domain.ErrReasonTransactionAlreadyReverted:
		txID, _ := strconv.ParseUint(metadata["transactionId"], 10, 64)
		return &domain.ErrTransactionAlreadyReverted{TransactionID: txID}

	case domain.ErrReasonInsufficientFunds:
		return &domain.ErrInsufficientFunds{
			Account: metadata["account"],
			Asset:   metadata["asset"],
			Amount:  metadata["amount"],
			Balance: metadata["balance"],
		}

	case domain.ErrReasonBalanceNotFound:
		return &domain.ErrBalanceNotFound{
			Account: metadata["account"],
			Asset:   metadata["asset"],
		}

	case domain.ErrReasonBalanceNotPreloaded:
		return &numscript.ErrBalanceNotPreloaded{
			Account: metadata["account"],
			Asset:   metadata["asset"],
		}

	case domain.ErrReasonNumscriptParseError:
		return &numscript.ErrNumscriptParse{Details: metadata["details"]}

	case domain.ErrReasonValidation:
		return fmt.Errorf("%s", message)

	default:
		return nil
	}
}
