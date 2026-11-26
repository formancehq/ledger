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

// Public reasons of a client signing rejection, carried in the gRPC ErrorInfo
// and as the HTTP errorCode.
const (
	SigningReasonMissingSignature = "MISSING_SIGNATURE"
	SigningReasonInvalidSignature = "INVALID_SIGNATURE"
	SigningReasonUnknownKeyID     = "UNKNOWN_KEY_ID"
)

// SigningStatus preserves the existing gRPC code and message and adds an
// ErrorInfo identifying a client signing rejection. Bare auth statuses remain
// transport failures: a follower must not report a cluster-secret failure as
// something its HTTP caller can fix.
func SigningStatus(err error) *status.Status {
	reason, ok := localSigningReason(err)
	if !ok {
		return nil
	}
	code := codes.PermissionDenied
	if reason == SigningReasonMissingSignature {
		code = codes.Unauthenticated
	}
	st, detailErr := status.New(code, err.Error()).WithDetails(&errdetails.ErrorInfo{Domain: signingErrorDomain, Reason: reason})
	if detailErr != nil {
		return status.New(codes.Internal, "encoding signing rejection")
	}

	return st
}

// SigningReason returns the public reason of a client signing rejection,
// whether raised locally or decoded from a peer's SigningStatus.
func SigningReason(err error) (string, bool) {
	if reason, ok := localSigningReason(err); ok {
		return reason, true
	}
	if failure, ok := errors.AsType[*signingFailure](err); ok {
		return failure.reason, true
	}

	return "", false
}

func localSigningReason(err error) (string, bool) {
	switch {
	case errors.Is(err, signing.ErrMissingSignature):
		return SigningReasonMissingSignature, true
	case errors.Is(err, signing.ErrInvalidSignature):
		return SigningReasonInvalidSignature, true
	case errors.Is(err, signing.ErrUnknownKeyID):
		return SigningReasonUnknownKeyID, true
	}

	return "", false
}

// signingFailure exposes only a coarse class and the signing reason, never
// peer diagnostic text. reconstructedError retains the original status.
type signingFailure struct {
	kind   domain.ErrorKind
	reason string
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
	switch reason := info.GetReason(); reason {
	case SigningReasonMissingSignature:
		if st.Code() == codes.Unauthenticated {
			return &signingFailure{kind: domain.KindUnauthenticated, reason: reason}
		}
	case SigningReasonInvalidSignature, SigningReasonUnknownKeyID:
		if st.Code() == codes.PermissionDenied {
			return &signingFailure{kind: domain.KindPermissionDenied, reason: reason}
		}
	}

	return nil
}
