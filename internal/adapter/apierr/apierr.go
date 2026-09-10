// Package apierr is the boundary contract every business-facing surface reads
// a failure through: the HTTP error handler, the bulk per-element mapper and
// ledgerctl.
//
// A failure reaching an API surface has one of two provenances. It was either
// raised locally — a domain.Describable from admission, the FSM or a read
// path, whose semantic ErrorKind is a pure function of its reason
// (domain.Kind) — or it was decoded from a peer, in which case the reason
// belongs to the *sending* build and this build's ErrorReason enum may not
// know it. A decoded failure therefore carries the classification it observed
// rather than re-deriving one, and Descriptor is the normalized view that
// hides which of the two it was.
//
// Two axes, deliberately separate. The semantic ErrorKind answers the client
// (an HTTP status, a CLI message); the gRPC status code is a transport-
// behaviour signal driving retry and hop behaviour. The mapping between them
// is lossy in both directions — KindConflict and KindPrecondition share
// codes.FailedPrecondition, so a code cannot name a kind — and a reason from a
// newer build carries a kind this enum cannot derive at all. Nothing in this
// package touches the transport axis: preserving the exact upstream status is
// the decoder's job (internal/adapter/grpcerr).
//
// This package is transport-neutral on purpose: it imports internal/domain and
// nothing else. HTTP reads a decoded remote failure through Descriptor without
// linking any gRPC detail, and the reverse direction is a layering violation —
// no package under internal/domain, internal/infra/state or internal/admission
// may import it, which scripts/check-repo-invariants enforces. A network-
// decoded failure must never be raised by admission, the FSM or order
// processing, never be persisted, frozen, audited or hashed: the audit chain
// hashes an error's Error() string, and a message that varies with a peer's
// build would break it.
package apierr

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// Descriptor is the normalized view of a failure: everything a business-facing
// consumer needs to answer a client, with the provenance erased. Obtain one
// with Describe rather than building it by hand.
type Descriptor struct {
	// Kind is the semantic classification the surface maps to a status code.
	Kind domain.ErrorKind

	// Reason is the stable, client-facing UPPER_SNAKE_CASE identifier.
	Reason string

	// Message is the client-safe human-readable message: the public
	// presentation the error type owns when it has one (domain.PublicDetails),
	// otherwise its Error(). Never the diagnostic identity of a type that
	// separates the two.
	Message string

	// Metadata is the client-safe structured per-occurrence context, or nil.
	// Same selection as Message.
	Metadata map[string]string

	// PublicOverride reports that the error type owns a public presentation
	// distinct from its diagnostic identity, so Message and Metadata are that
	// redacted view. A consumer that would otherwise render the whole error
	// chain — outer wrapping text included — must render Message instead:
	// the wrapping was built from Error(), which is what the override exists
	// to withhold.
	PublicOverride bool
}

// Remote is a failure decoded from a peer: the wire contract (reason,
// metadata, message) plus the semantic kind the sender described, held without
// committing the receiver to any concrete Go type. Reconstructing concrete
// types from a reason switch would build a second, version-sensitive shadow
// taxonomy; this carries the contract instead, so a server-side error type
// added tomorrow reaches this receiver with full structured information and no
// code change here.
//
// It is produced only by a decoder at a transport boundary — today
// internal/adapter/grpcerr, for the CLI decoding a status from the server and
// for a follower decoding a status from the leader while forwarding.
//
// Remote satisfies domain.Describable so a consumer not yet migrated to
// Describe still sees a typed error rather than an unrecognised one; such a
// consumer re-derives the kind from the reason and loses the wire's
// classification for a reason this build does not know. Describe is the
// contract that does not.
type Remote struct {
	// KindValue is the semantic kind observed off the wire.
	KindValue domain.ErrorKind

	// ReasonValue is the reason the sender stamped.
	ReasonValue string

	// Msg is the sender's client-safe message.
	Msg string

	// Meta is the sender's structured metadata, or nil.
	Meta map[string]string
}

var _ domain.Describable = (*Remote)(nil)

func (e *Remote) Error() string               { return e.Msg }
func (e *Remote) Reason() string              { return e.ReasonValue }
func (e *Remote) Metadata() map[string]string { return e.Meta }

// InvalidWireError reports a decoded failure whose reason and transport code
// contradict each other: a reason this build knows, carried under a code that
// build could not have produced for it. That is a protocol fault, not a
// business outcome, so nothing received is trusted — the reason, message and
// metadata are dropped rather than answered to the client.
//
// It deliberately implements neither domain.Describable nor GRPCStatus, so it
// reaches the internal-error sanitizer on every surface: an HTTP 500 with a
// correlation ID and a server-side log, and codes.Unknown with a correlation
// ID on gRPC. Error() names the offending pair for that log and withholds the
// untrusted payload.
type InvalidWireError struct {
	// ReasonValue is the reason received, recorded for the server-side log.
	ReasonValue string

	// Code is the transport code it arrived under.
	Code codes.Code

	// Expected lists the codes this build would have sent that reason under.
	Expected []codes.Code
}

func (e *InvalidWireError) Error() string {
	return fmt.Sprintf(
		"invalid wire error: reason %s arrived as %s, expected %v",
		e.ReasonValue, e.Code, e.Expected,
	)
}

// InvalidWire reports whether err's chain holds an *InvalidWireError.
//
// Every client-facing surface must branch on this before rendering anything
// derived from err, and answer its own internal-error representation instead.
// An invalid pair means the sender is not a ledger of this contract, so
// nothing it sent is trustworthy — least of all its free-form message and
// metadata, which must never reach a client as though this build had produced
// them. Error() above is the safe rendering: it names the reason and codes,
// which are this build's own enum values, and carries no peer text.
//
// The type deliberately implements neither Describable nor GRPCStatus, so a
// surface that omits this check still degrades to its internal-error path
// rather than answering the pair as a business outcome. This function makes
// that guarantee explicit rather than incidental to the method set.
func InvalidWire(err error) (*InvalidWireError, bool) {
	return errors.AsType[*InvalidWireError](err)
}

// Describe returns the normalized view of err, reading either a decoded remote
// failure or a locally raised domain.Describable, and reports false when the
// chain holds neither.
//
// A *Remote is checked first and its carried kind wins. Falling through to the
// Describable branch would re-derive the kind from the reason, which is exactly
// what must not happen for a reason this build's enum does not know:
// ReasonCode yields UNSPECIFIED, KindForReason collapses it to KindInternal,
// and a caller mistake becomes a 500.
//
// A locally raised failure is read through domain.PublicErrorDetails, so a type
// that separates its diagnostic identity from its client presentation never
// reaches a surface through this contract with the diagnostic one. A decoded
// remote failure needs no such selection: the sender already applied it before
// serialising (internal/adapter/grpc.describableToGRPCStatus), so what arrived
// is the public presentation and PublicOverride stays false. Consumers render
// Message on public paths so outer routing context cannot change the response.
func Describe(err error) (Descriptor, bool) {
	if remote, ok := errors.AsType[*Remote](err); ok {
		return Descriptor{
			Kind:     remote.KindValue,
			Reason:   remote.ReasonValue,
			Message:  remote.Msg,
			Metadata: remote.Meta,
		}, true
	}

	if d, ok := errors.AsType[domain.Describable](err); ok {
		message, metadata, overridden := domain.PublicErrorDetails(d)

		return Descriptor{
			Kind:           domain.Kind(d),
			Reason:         d.Reason(),
			Message:        message,
			Metadata:       metadata,
			PublicOverride: overridden,
		}, true
	}

	return Descriptor{}, false
}
