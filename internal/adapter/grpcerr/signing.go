package grpcerr

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
)

const signingErrorDomain = "ledger.signing"

// SigningStatus preserves the existing gRPC code and message and adds an
// ErrorInfo identifying a client signing rejection. Bare auth statuses remain
// transport failures: a follower must not report a cluster-secret failure as
// something its HTTP caller can fix.
func SigningStatus(err error) *status.Status {
	var reason string
	code := codes.PermissionDenied
	switch {
	case errors.Is(err, signing.ErrMissingSignature):
		reason, code = "MISSING_SIGNATURE", codes.Unauthenticated
	case errors.Is(err, signing.ErrInvalidSignature):
		reason = "INVALID_SIGNATURE"
	case errors.Is(err, signing.ErrUnknownKeyID):
		reason = "UNKNOWN_KEY_ID"
	default:
		return nil
	}
	st, detailErr := status.New(code, err.Error()).WithDetails(&errdetails.ErrorInfo{Domain: signingErrorDomain, Reason: reason})
	if detailErr != nil {
		return status.New(codes.Internal, "encoding signing rejection")
	}

	return st
}

// signingFailure exposes only a coarse class, never peer diagnostic text or
// a public business reason. reconstructedError retains the original status.
type signingFailure struct {
	kind domain.ErrorKind
}

func (e *signingFailure) Error() string          { return domain.PublicKindMessage(e.kind) }
func (e *signingFailure) Kind() domain.ErrorKind { return e.kind }

func decodeSigningStatus(st *status.Status) error {
	details := st.Details()
	if len(details) != 1 {
		return nil
	}
	info, ok := details[0].(*errdetails.ErrorInfo)
	if !ok || info.GetDomain() != signingErrorDomain {
		return nil
	}
	switch info.GetReason() {
	case "MISSING_SIGNATURE":
		if st.Code() == codes.Unauthenticated {
			return &signingFailure{kind: domain.KindUnauthenticated}
		}
	case "INVALID_SIGNATURE", "UNKNOWN_KEY_ID":
		if st.Code() == codes.PermissionDenied {
			return &signingFailure{kind: domain.KindPermissionDenied}
		}
	}

	return nil
}
