package cmdutil

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// CLIError wraps an error that has already been displayed to the user.
// The central error handler in main.go checks for this to avoid duplicate output.
type CLIError struct{ Err error }

func (e *CLIError) Error() string { return e.Err.Error() }
func (e *CLIError) Unwrap() error { return e.Err }

// Displayed wraps an error to mark it as already shown to the user.
func Displayed(err error) error {
	if err == nil {
		return nil
	}

	return &CLIError{Err: err}
}

// FormatGRPCError prints a clean error for gRPC errors and returns a Displayed error.
// For business errors, it reconstructs the typed error and prints details.
// For other gRPC errors, it uses a human-friendly message based on the status code.
// For non-gRPC errors, it wraps the original error.
func FormatGRPCError(context string, err error) error {
	bizErr := BusinessErrorFromGRPC(err)
	if bizErr != nil {
		msg := fmt.Sprintf("%s: %s", context, bizErr.Err.Error())
		pterm.Error.Println(msg)
		printErrorDetails(bizErr.Err)

		return Displayed(fmt.Errorf("%s", msg))
	}

	// status.Convert always returns a status (wrapping non-gRPC errors as Unknown).
	// This handles streaming errors that don't implement GRPCStatus() directly.
	st := status.Convert(err)
	if st.Code() != codes.OK {
		if msg := friendlyMessage(st); msg != "" {
			formatted := fmt.Sprintf("%s: %s", context, msg)
			pterm.Error.Println(formatted)

			return Displayed(fmt.Errorf("%s", formatted))
		}
	}

	msg := fmt.Sprintf("%s: %s", context, err.Error())
	pterm.Error.Println(msg)

	return Displayed(fmt.Errorf("%s", msg))
}

// friendlyMessage returns a human-readable message for common gRPC status codes.
func friendlyMessage(st *status.Status) string {
	switch st.Code() {
	case codes.Unauthenticated:
		return formatAuthError(st.Message())
	case codes.PermissionDenied:
		return "access denied: " + st.Message()
	case codes.Unavailable:
		return "server unavailable: " + st.Message()
	case codes.DeadlineExceeded:
		return "request timed out"
	case codes.Unimplemented:
		return "not supported by server: " + st.Message()
	default:
		return st.Message()
	}
}

// formatAuthError returns a helpful message for authentication failures,
// including the specific reason from the server when available.
func formatAuthError(serverMsg string) string {
	if serverMsg == "" {
		return "authentication failed (check your credentials or token)"
	}

	msg := "authentication failed: " + serverMsg

	// Add hints for common failure reasons.
	switch {
	case strings.Contains(serverMsg, "expired"):
		msg += "\nhint: generate a new token with 'ledgerctl auth token'"
	case strings.Contains(serverMsg, "signature"):
		msg += "\nhint: verify the signing key matches the server's configuration"
	case strings.Contains(serverMsg, "missing"):
		msg += "\nhint: set a token with 'ledgerctl auth token' or --token flag"
	}

	return msg
}

// printErrorDetails prints structured details for specific business error types.
func printErrorDetails(err error) {
	var (
		insufficient *domain.ErrInsufficientFunds
		refConflict  *domain.ErrTransactionReferenceConflict
		indexMissing *domain.ErrIndexNotFound
		indexBuild   *domain.ErrIndexBuilding
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
	case errors.As(err, &indexMissing):
		pterm.Println()
		pterm.Printf("  Index: %s\n", pterm.Yellow(indexMissing.Index))
		pterm.Println(pterm.Gray("  hint: create the index with 'ledgerctl indexes create'"))
	case errors.As(err, &indexBuild):
		pterm.Println()
		pterm.Printf("  Index: %s\n", pterm.Yellow(indexBuild.Index))
		pterm.Println(pterm.Gray("  hint: wait for the index to finish building, check status with 'ledgerctl indexes list'"))
	}
}

// BusinessErrorFromGRPC extracts a BusinessError from a gRPC status error.
// Returns nil if the error is not a business error (no ErrorInfo with domain "ledger").
func BusinessErrorFromGRPC(err error) *domain.BusinessError {
	st := status.Convert(err)
	if st.Code() == codes.OK {
		return nil
	}

	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != "ledger" {
			continue
		}

		inner := reconstructError(info.GetReason(), info.GetMetadata(), st.Message())
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

	case domain.ErrReasonLedgerDeleted:
		return &domain.ErrLedgerDeleted{Name: metadata["name"]}

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
		return &domain.ErrBalanceNotPreloaded{
			Account: metadata["account"],
			Asset:   metadata["asset"],
		}

	case domain.ErrReasonNumscriptParseError:
		return &domain.ErrNumscriptParse{Details: metadata["details"]}

	case domain.ErrReasonValidation:
		return fmt.Errorf("%s", message)

	case domain.ErrReasonIndexNotFound:
		return &domain.ErrIndexNotFound{Index: metadata["index"]}

	case domain.ErrReasonIndexBuilding:
		return &domain.ErrIndexBuilding{Index: metadata["index"]}

	default:
		return nil
	}
}
