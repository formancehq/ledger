package numscript

import (
	"context"
	"errors"
	"fmt"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// convertNumscriptError translates known numscript library errors raised by
// dependency resolution (its only caller, SafeResolveDependencies) into domain
// errors so that the gRPC error mapper can return proper status codes.
// Resolution does no balance arithmetic, so a missing-funds failure cannot
// originate here; execution errors, including missing funds, go through the
// VM's convertVMError. Library errors that have no specific mapping are
// wrapped as ErrNumscriptRuntime (KindInternal) — an unhandled failure mode,
// which is a server bug the user cannot fix.
func convertNumscriptError(err error) domain.SerializableError {
	if err == nil {
		return nil
	}

	// Asset scaling (`… with scaling through …`) is not supported by dependency
	// resolution: SourceWithScaling returns ErrScalingNotSupported unconditionally,
	// independent of any balance/metadata. It is the one deterministic resolver
	// failure the library re-exports as a public sentinel (numscriptlib.
	// ErrScalingNotSupported), so — unlike the internal-only residue below — we can
	// split it out without importing internals or matching strings. Map it to the
	// freezable ErrNumscriptScalingUnsupported (KindValidation) so admission
	// terminates it definitively rather than forwarding a PRELOAD_UNAVAILABLE that
	// no retry could satisfy. This closes the read-then-scaling loop that the
	// provenance flag alone could not, because a successful balance()/meta() origin
	// read (bound before statements are walked) sets MutableReadAttempted before the
	// scaling source deterministically fails (EN-1557).
	if errors.Is(err, numscriptlib.ErrScalingNotSupported) {
		return domain.ErrNumscriptScalingUnsupported
	}

	// errors.As walks the chain in case a caller has already wrapped the
	// numscript-library error in a typed domain failure. This also unwraps
	// QueryBalanceError / QueryMetadataError, whose WrappedError is the Store
	// error — so a rejected scoped read (domain.ErrScopedBalanceUnsupported)
	// surfaces here as the validation sentinel it already is.
	if d, ok := errors.AsType[domain.SerializableError](err); ok {
		return d
	}

	// Every other library error becomes ErrNumscriptRuntime (KindInternal).
	//
	// This intentionally keeps a single conservative classification for the whole
	// residue. Admission no longer needs the leaf error category to decide
	// forward-vs-terminate: it classifies from state provenance instead — selector
	// mutability (`latest` vs inline/exact) plus whether resolution attempted a
	// mutable balance/metadata read (RecordingStore.MutableReadAttempted, carried
	// out via DependencyResolutionError). See EN-1557. This matters because the
	// upstream library reports script-deterministic errors and state-dependent ones
	// (e.g. MetadataNotFound when a meta()-referenced account was deleted after an
	// earlier success) with the same leaf InterpreterError shape, and the concrete
	// types live in an internal package we must not import. A public Numscript
	// resolver-error taxonomy is therefore NOT required (EN-1563 cancelled) — the
	// one publicly-exposed deterministic sentinel (scaling) is handled above; the
	// rest stay conservatively forwarded — and we must never classify by error
	// string or import Numscript internals.
	return &domain.ErrNumscriptRuntime{Detail: err.Error()}
}

// panicError marks a Describable that originated from a recovered panic inside
// the numscript library (as opposed to a normal library-returned error). It
// behaves exactly like the ErrNumscriptRuntime it wraps for every external
// concern — same Error/Reason/Metadata, so the gRPC/HTTP mapping is unchanged —
// but IsPanic can recognise it so a caller that would otherwise soften a normal
// resolution error (e.g. the FSM apply path funnelling resolve errors to
// ErrStaleInputsResolution) can instead surface the panic loudly. A recovered
// panic is a "should not happen" (invariant #7): masking it as a retryable
// stale-inputs error would hide a real defect.
type panicError struct {
	*domain.ErrNumscriptRuntime
}

// Unwrap exposes the embedded ErrNumscriptRuntime so errors.As(err,
// **domain.ErrNumscriptRuntime) succeeds: a recovered panic must present
// externally as an ordinary ErrNumscriptRuntime (same Reason/Kind/mapping),
// while IsPanic keeps it internally distinguishable.
func (e panicError) Unwrap() error { return e.ErrNumscriptRuntime }

// IsPanic reports whether err was produced by a numscript-library panic that a
// Safe* wrapper recovered, rather than by a normal library-returned error.
func IsPanic(err error) bool {
	var pe panicError

	return errors.As(err, &pe)
}

// numscriptPanicToDescribable maps a value recovered from a panic inside the
// numscript library into a Describable panicError (which behaves as an
// ErrNumscriptRuntime). It returns nil when recovered is nil (no panic in
// flight). Callers invoke recover() themselves — directly inside their own
// deferred closure, as the Go runtime requires — and pass the result here so
// the panic→Describable conversion lives in one place (DRY) across every Safe*
// wrapper.
func numscriptPanicToDescribable(recovered any) domain.SerializableError {
	if recovered == nil {
		return nil
	}

	return panicError{&domain.ErrNumscriptRuntime{Detail: fmt.Sprintf("numscript panic: %v", recovered)}}
}

// SafeResolveDependencies wraps ParseResult.ResolveDependencies with a
// deferred recover and the shared error conversion. ResolveDependencies
// runs untrusted script analysis and can panic on adversarial input; both call
// sites (admission dependency discovery and — critically — the FSM apply-path
// stale-inputs re-resolution) must never let that panic escape, so the panic is
// converted into a Describable ErrNumscriptRuntime rather than crashing the
// request goroutine or the Raft apply loop. Library errors are mapped through
// the shared convertNumscriptError.
func SafeResolveDependencies(parsed numscriptlib.ParseResult, ctx context.Context, vars numscriptlib.VariablesMap, store numscriptlib.Store) (resolved numscriptlib.ResolvedDependencies, err domain.SerializableError) {
	defer func() {
		if panicErr := numscriptPanicToDescribable(recover()); panicErr != nil {
			resolved = numscriptlib.ResolvedDependencies{}
			err = panicErr
		}
	}()

	resolved, resolveErr := parsed.ResolveDependencies(ctx, vars, store)
	err = convertNumscriptError(resolveErr)

	return
}
