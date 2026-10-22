package http

import (
	"errors"
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// kindToHTTPStatus maps a semantic ErrorKind to an HTTP status code. Adding
// a new Kind without a branch fails the `exhaustive` golangci-lint rule;
// that's the structural guarantee that drives this design (#431).
func kindToHTTPStatus(k domain.ErrorKind) int {
	switch k { //exhaustive:enforce
	case domain.KindValidation:
		return http.StatusBadRequest
	case domain.KindNotFound:
		return http.StatusNotFound
	case domain.KindAlreadyExists:
		return http.StatusConflict
	case domain.KindConflict:
		return http.StatusConflict
	case domain.KindPrecondition:
		return http.StatusBadRequest
	case domain.KindUnavailable:
		return http.StatusServiceUnavailable
	case domain.KindUnauthenticated:
		return http.StatusUnauthorized
	case domain.KindPermissionDenied:
		return http.StatusForbidden
	case domain.KindInternal:
		return http.StatusInternalServerError
	case domain.KindResourceExhausted:
		return http.StatusTooManyRequests
	}

	// Unreachable when the exhaustive linter is enabled.
	return http.StatusInternalServerError
}

// classifiedErrorCode is the coarse error code for a failure that classifies
// itself but owns no public Reason. It names the class and nothing finer: a
// domain.Classifiable never committed to a wire identifier, so deriving a
// reason-shaped code from its Go type here would ship a contract the error
// deliberately declined to make. These are the same generic codes this handler
// already answers reason-less statuses with.
func classifiedErrorCode(k domain.ErrorKind) string {
	switch k { //exhaustive:enforce
	case domain.KindValidation, domain.KindPrecondition:
		return "INVALID_REQUEST"
	case domain.KindNotFound:
		return "NOT_FOUND"
	case domain.KindAlreadyExists, domain.KindConflict:
		return "CONFLICT"
	case domain.KindUnavailable:
		return "UNAVAILABLE"
	case domain.KindUnauthenticated:
		return "UNAUTHENTICATED"
	case domain.KindPermissionDenied:
		return "PERMISSION_DENIED"
	case domain.KindResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case domain.KindInternal:
		return "INTERNAL_ERROR"
	}

	// Unreachable when the exhaustive linter is enabled.
	return "INTERNAL_ERROR"
}

// errorClass is the public HTTP rendering of a server-side error. Single
// requests and bulk elements both derive theirs from classifyError, so one
// failure answers the same status, code and message on either surface.
type errorClass struct {
	status int
	code   string
	// message is the client-visible text. Empty means the error must not be
	// echoed: publicMessage answers the generic correlated internal error.
	message string
}

// publicMessage returns the client-visible message. A 500 is recorded under
// the request's correlation ID, which a sanitized message then carries.
func (c errorClass) publicMessage(r *http.Request, err error) string {
	if c.status != http.StatusInternalServerError {
		return c.message
	}

	id := correlationID(r)
	recordHTTPInternalError(r, id, err)
	if c.message != "" {
		return c.message
	}

	return fmt.Sprintf("internal server error (correlation ID: %s)", id)
}

// handleError converts a server-side error into a JSON-formatted HTTP
// response. Every 503 carries Retry-After: an unavailable answer is by
// definition a retry-now condition.
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	c := classifyError(err)
	if c.message == "" {
		writeInternalServerError(w, r, err)

		return
	}

	if c.status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}

	writeErrorResponse(w, c.status, c.code, errors.New(c.publicMessage(r, err)))
}

// classifyError selects the public rendering of err. The bulk of the work is
// delegated to the domain error contract: any *Err* type or sentinel from
// internal/domain flows through kindToHTTPStatus + Reason(), and an error that
// classifies itself without declaring a reason answers from its kind alone.
// The few branches below handle errors that carry neither — the
// leader-discovery sentinel from commonpb and the generic NotFoundError used
// by route lookups.
func classifyError(err error) errorClass {
	// commonpb.ErrNoLeader carries its own retry hint (Retry-After) — keep
	// the dedicated branch so the response shape stays stable for clients
	// that have been told to honour it.
	if errors.Is(err, commonpb.ErrNoLeader) {
		return errorClass{status: http.StatusServiceUnavailable, code: "NO_LEADER", message: err.Error()}
	}

	// plan.ErrCacheHorizonExceeded is an infrastructure-level admission
	// rejection (not a domain business outcome, hence not a Describable):
	// admission predicted 2+ cache rotations between propose and apply so
	// any preload would be discarded before FSM read. Retry against a fresh
	// admission snapshot. Mirrors the gRPC adapter's codes.Unavailable
	// mapping (see internal/adapter/grpc/server.go).
	if errors.Is(err, plan.ErrCacheHorizonExceeded) {
		return errorClass{status: http.StatusServiceUnavailable, code: "CACHE_HORIZON_EXCEEDED", message: err.Error()}
	}

	if _, ok := errors.AsType[*commonpb.NotFoundError](err); ok {
		return errorClass{status: http.StatusNotFound, code: "NOT_FOUND", message: err.Error()}
	}

	// domain.ErrNotFound is a bare sentinel (not a typed NotFoundError and not
	// a Describable), returned by read-side queries when the target ledger or
	// resource is absent — e.g. reading numscripts or prepared-queries against
	// a deleted ledger via query.GetLedgerByName. Without this branch it falls
	// through to the 500 sanitizer; a missing resource on a read is a 404.
	if errors.Is(err, domain.ErrNotFound) {
		return errorClass{status: http.StatusNotFound, code: "NOT_FOUND", message: err.Error()}
	}

	// A reason this build knows arriving under a code it would never send that
	// reason under: the sender is not a ledger of this contract. Answer the
	// generic sanitizer so none of the reason, message or metadata it claimed
	// is echoed. InvalidWireError implements neither Describable nor
	// GRPCStatus, so it would reach the sanitizer below anyway; this branch
	// states the contract instead of relying on that method set.
	if _, ok := apierr.InvalidWire(err); ok {
		return errorClass{status: http.StatusInternalServerError, code: "INTERNAL_ERROR"}
	}

	// The boundary contract: every typed *Err* and sentinel in internal/domain
	// (and transitively in admission/numscript) flows through this branch, as
	// does a failure decoded from the leader. apierr.Describe normalises the
	// two provenances — a locally raised Describable classifies from its
	// reason, a decoded one keeps the kind the wire carried, which is what
	// preserves a reason this build's enum does not know. Catches
	// BusinessError too (it implements Describable transparently). The
	// message is the public one selected by the originating gRPC encoder;
	// routing wrappers belong to diagnostics, not the response.
	if d, ok := apierr.Describe(err); ok {
		return errorClass{status: kindToHTTPStatus(d.Kind), code: d.Reason, message: d.Message}
	}

	// A failure that classifies itself but owns no public reason: the read
	// path's prepared-query argument checks, the admission idempotency-key
	// guards. Describe cannot normalise it — there is no Reason to normalise —
	// but the kind still selects the status, so a caller mistake answers 400
	// here exactly as it answers codes.InvalidArgument on the gRPC surface.
	// KindInternal keeps the sanitiser: a server fault must not echo its
	// message to the client.
	if c, ok := errors.AsType[domain.Classifiable](err); ok && c.Kind() != domain.KindInternal {
		return errorClass{
			status:  kindToHTTPStatus(c.Kind()),
			code:    classifiedErrorCode(c.Kind()),
			message: domain.PublicKindMessage(c.Kind()),
		}
	}

	// Statuses with nothing typed left to recover. A business error forwarded
	// from the leader no longer reaches here: grpcerr.NewConn rebuilds it into a
	// Describable at the transport seam (see bootstrap.getLeaderCtrl), so it is
	// caught by the branch above with its real reason and status. What is left
	// are statuses that never carried an ErrorInfo.
	//
	// A codes.InvalidArgument is a caller error, not a server fault, and must
	// not degrade to a 500. The audit-filter compiler (query.CompileAuditFilter,
	// shared with the gRPC surface) rejects filters that parse but are not
	// audit-supported — `not outcome == failure`, a non-audit condition, an
	// unknown field — as codes.InvalidArgument; surface that as a 400. A
	// codes.Unavailable status is a retry-now transient (a forwarded stream torn
	// down mid-transfer, a peer connection missing from the pool, no leader yet)
	// and gets the same 503 + Retry-After contract as the KindUnavailable
	// Describables above; that class has no reason code to recover, which is why
	// the seam leaves it alone.
	//
	// Only these two codes are translated; the rest keep the generic 500
	// fallthrough deliberately, since they denote genuine server faults.
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument:
			return errorClass{status: http.StatusBadRequest, code: "INVALID_REQUEST", message: "invalid request"}
		case codes.Unavailable:
			return errorClass{status: http.StatusServiceUnavailable, code: "UNAVAILABLE", message: "unavailable"}
		}
	}

	return errorClass{status: http.StatusInternalServerError, code: "INTERNAL_ERROR"}
}
