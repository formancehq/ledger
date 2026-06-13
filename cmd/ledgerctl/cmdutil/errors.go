package cmdutil

import (
	"errors"
	"fmt"
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

// printErrorDetails prints structured details for business errors. After
// the Describable refactor (#431) reconstructed errors carry Reason+Metadata
// directly off the wire; this switch dispatches on Reason rather than Go
// type so a new server-side error gets a "no extra details" graceful
// fallback instead of a missing branch.
func printErrorDetails(err error) {
	var d domain.Describable
	if !errors.As(err, &d) {
		return
	}

	meta := d.Metadata()

	switch d.Reason() {
	case domain.ErrReasonInsufficientFunds:
		pterm.Println()
		pterm.Printf("  Account: %s\n", pterm.Cyan(meta["account"]))
		pterm.Printf("  Asset:   %s\n", pterm.Yellow(meta["asset"]))
		pterm.Printf("  Balance: %s\n", pterm.Red(meta["balance"]))
		pterm.Printf("  Needed:  %s\n", meta["amount"])
	case domain.ErrReasonTransactionReferenceConflict:
		pterm.Println()
		pterm.Printf("  Reference: %s\n", pterm.Cyan(meta["reference"]))
	case domain.ErrReasonIndexNotFound:
		pterm.Println()
		pterm.Printf("  Index: %s\n", pterm.Yellow(meta["index"]))
		pterm.Println(pterm.Gray("  hint: create the index with 'ledgerctl indexes create'"))
	case domain.ErrReasonIndexBuilding:
		pterm.Println()
		pterm.Printf("  Index: %s\n", pterm.Yellow(meta["index"]))
		pterm.Println(pterm.Gray("  hint: wait for the index to finish building, check status with 'ledgerctl indexes list'"))
	}
}

// BusinessErrorFromGRPC extracts a BusinessError from a gRPC status error.
// Returns nil if the error is not a business error (no ErrorInfo with
// domain "ledger"). The returned BusinessError.Err is a *domain.RemoteError
// transporting the wire contract (Reason, Metadata, Message) plus the
// Kind derived from the gRPC status code. New server-side error types
// reach this code path automatically — no client-side switch to extend.
func BusinessErrorFromGRPC(err error) *domain.BusinessError {
	st := status.Convert(err)
	if st.Code() == codes.OK {
		return nil
	}

	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != "ledger" || info.GetReason() == "" {
			continue
		}

		return &domain.BusinessError{
			Err: &domain.RemoteError{
				KindValue:   grpcCodeToKind(st.Code()),
				ReasonValue: info.GetReason(),
				Message:     st.Message(),
				Meta:        info.GetMetadata(),
			},
		}
	}

	return nil
}

// grpcCodeToKind reverses the server-side kindToGRPCCode mapping. Two Kinds
// (KindConflict, KindPrecondition) collapse to codes.FailedPrecondition on
// the wire; the client cannot distinguish them post-fact, so we conservatively
// pick KindPrecondition (the more common semantic). Clients that need the
// distinction should pattern-match on Reason instead.
func grpcCodeToKind(c codes.Code) domain.ErrorKind {
	switch c {
	case codes.InvalidArgument:
		return domain.KindValidation
	case codes.NotFound:
		return domain.KindNotFound
	case codes.AlreadyExists:
		return domain.KindAlreadyExists
	case codes.FailedPrecondition:
		return domain.KindPrecondition
	case codes.Unavailable:
		return domain.KindUnavailable
	case codes.Unauthenticated:
		return domain.KindUnauthenticated
	case codes.PermissionDenied:
		return domain.KindPermissionDenied
	default:
		return domain.KindInternal
	}
}
