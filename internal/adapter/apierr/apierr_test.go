package apierr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// TestDescribe_LocalDescribableClassifiesFromItsReason pins the local
// provenance: a domain error's kind comes from domain.Kind, which is a pure
// function of its reason.
func TestDescribe_LocalDescribableClassifiesFromItsReason(t *testing.T) {
	t.Parallel()

	err := &domain.ErrLedgerDeleted{Name: "foo"}

	d, ok := Describe(err)
	require.True(t, ok)
	require.Equal(t, domain.KindConflict, d.Kind)
	require.Equal(t, domain.ErrReasonLedgerDeleted, d.Reason)
	require.Equal(t, err.Error(), d.Message)
	require.Equal(t, err.Metadata(), d.Metadata)
}

// TestDescribe_BusinessErrorUnwraps: domain.BusinessError is transparent, so a
// wrapped domain error describes exactly as the inner one does.
func TestDescribe_BusinessErrorUnwraps(t *testing.T) {
	t.Parallel()

	inner := &domain.ErrInsufficientFunds{Account: "a", Asset: "USD", Amount: "10", Balance: "5"}

	d, ok := Describe(&domain.BusinessError{Err: inner})
	require.True(t, ok)
	require.Equal(t, domain.KindPrecondition, d.Kind)
	require.Equal(t, domain.ErrReasonInsufficientFunds, d.Reason)
}

// TestDescribe_RemoteKeepsTheKindItCarries is the reason this contract exists.
// A decoded failure whose reason this build's enum does not know must keep the
// classification the sender described. Re-deriving it — which is what the
// Describable branch would do — yields KindInternal and turns a caller mistake
// into a 500.
func TestDescribe_RemoteKeepsTheKindItCarries(t *testing.T) {
	t.Parallel()

	const unknownReason = "SOME_REASON_FROM_A_NEWER_SERVER"

	require.Equal(t, domain.KindInternal,
		domain.KindForReason(domain.ReasonCode(unknownReason)),
		"precondition: re-deriving this reason must collapse to KindInternal, "+
			"otherwise the test cannot distinguish the two branches")

	remote := &Remote{
		KindValue:   domain.KindAlreadyExists,
		ReasonValue: unknownReason,
		Msg:         "something conflicted",
		Meta:        map[string]string{"name": "foo"},
	}

	d, ok := Describe(remote)
	require.True(t, ok)
	require.Equal(t, domain.KindAlreadyExists, d.Kind, "the carried kind must win")
	require.Equal(t, unknownReason, d.Reason)
	require.Equal(t, "something conflicted", d.Message)
	require.Equal(t, map[string]string{"name": "foo"}, d.Metadata)
}

// TestDescribe_RemoteIsFoundThroughAWrapper: the decoder wraps the Remote in a
// carrier that keeps the original gRPC status reachable, so Describe has to
// walk the chain rather than type-assert the top value.
func TestDescribe_RemoteIsFoundThroughAWrapper(t *testing.T) {
	t.Parallel()

	remote := &Remote{
		KindValue:   domain.KindConflict,
		ReasonValue: domain.ErrReasonLedgerDeleted,
		Msg:         "ledger deleted: foo",
	}

	d, ok := Describe(fmt.Errorf("forwarding to leader: %w", remote))
	require.True(t, ok)
	require.Equal(t, domain.KindConflict, d.Kind)
}

// TestDescribe_RemoteWinsOverTheDescribableBranch pins the branch order. A
// Remote satisfies domain.Describable too, so if the branches were swapped the
// kind would be silently re-derived from the reason and the wire's
// classification lost.
func TestDescribe_RemoteWinsOverTheDescribableBranch(t *testing.T) {
	t.Parallel()

	// LEDGER_DELETED is KindConflict by reason. Claiming KindAlreadyExists is
	// not something a real sender would do; it is here so the two branches
	// answer differently and the assertion can tell them apart.
	remote := &Remote{
		KindValue:   domain.KindAlreadyExists,
		ReasonValue: domain.ErrReasonLedgerDeleted,
		Msg:         "ledger deleted: foo",
	}

	require.Equal(t, domain.KindConflict, domain.Kind(remote),
		"precondition: the Describable branch would answer KindConflict here")

	d, ok := Describe(remote)
	require.True(t, ok)
	require.Equal(t, domain.KindAlreadyExists, d.Kind)
}

// TestDescribe_UnrecognisedError: an infrastructure failure has no business
// classification, and the caller must be told so rather than handed a
// defaulted kind.
func TestDescribe_UnrecognisedError(t *testing.T) {
	t.Parallel()

	_, ok := Describe(errors.New("dial tcp: connection refused"))
	require.False(t, ok)

	_, ok = Describe(nil)
	require.False(t, ok)
}

// TestInvalidWireError_IsNotABusinessOutcome: a contradicting reason/code pair
// must reach the internal-error sanitizer, which means satisfying neither the
// boundary contract nor domain.Describable.
func TestInvalidWireError_IsNotABusinessOutcome(t *testing.T) {
	t.Parallel()

	invalid := &InvalidWireError{
		ReasonValue: domain.ErrReasonLedgerNotFound,
		Code:        codes.AlreadyExists,
		Expected:    []codes.Code{codes.NotFound},
	}

	_, ok := Describe(invalid)
	require.False(t, ok, "a protocol fault is not a business outcome")

	// A direct assertion on the value, not the chain: the invariant is that
	// this type never satisfies the domain contract.
	_, isDescribable := any(invalid).(domain.Describable)
	require.False(t, isDescribable)

	require.Contains(t, invalid.Error(), domain.ErrReasonLedgerNotFound)
	require.Contains(t, invalid.Error(), codes.AlreadyExists.String())
}

// TestRemote_SatisfiesDescribable: a consumer that has not been migrated to
// Describe still sees a typed error. It loses the carried kind, which is why
// Describe exists, but it must not fall through to "unknown error".
func TestRemote_SatisfiesDescribable(t *testing.T) {
	t.Parallel()

	var d domain.Describable = &Remote{
		KindValue:   domain.KindConflict,
		ReasonValue: domain.ErrReasonLedgerDeleted,
		Msg:         "ledger deleted: foo",
		Meta:        map[string]string{"name": "foo"},
	}

	require.Equal(t, domain.ErrReasonLedgerDeleted, d.Reason())
	require.Equal(t, "ledger deleted: foo", d.Error())
	require.Equal(t, map[string]string{"name": "foo"}, d.Metadata())
}

// TestDescribe_LocalDescribableUsesItsPublicPresentation pins the EN-1623
// redaction contract at this boundary: a type that separates its diagnostic
// identity from its client presentation must reach a surface through Describe
// with the public one, and the flag must tell the surface to render Message
// instead of the wrapped chain it was built from.
func TestDescribe_LocalDescribableUsesItsPublicPresentation(t *testing.T) {
	t.Parallel()

	inconsistent := &domain.ErrIndexInconsistent{Index: "secret-index", Detail: "raw storage detail"}

	d, ok := Describe(fmt.Errorf("read accounts: %w", inconsistent))
	require.True(t, ok)

	require.True(t, d.PublicOverride, "the type owns a separate public presentation")
	require.Equal(t, "index is inconsistent", d.Message)
	require.Nil(t, d.Metadata)
	require.NotContains(t, d.Message, "secret-index")
	require.Equal(t, domain.ErrReasonIndexInconsistent, d.Reason,
		"the reason is the wire contract and is never redacted")
}

// TestDescribe_LocalDescribableWithoutPublicPresentation: the common case is
// unchanged — the describable Error() and Metadata() are the client-facing
// values, without an explicit public presentation override.
func TestDescribe_LocalDescribableWithoutPublicPresentation(t *testing.T) {
	t.Parallel()

	d, ok := Describe(&domain.ErrLedgerDeleted{Name: "foo"})
	require.True(t, ok)

	require.False(t, d.PublicOverride)
	require.Equal(t, (&domain.ErrLedgerDeleted{Name: "foo"}).Error(), d.Message)
}

// TestDescribe_RemoteNeedsNoPublicSelection: the sender applied its own public
// presentation before serialising, so what arrived is already client-safe and
// the receiver has nothing left to select.
func TestDescribe_RemoteNeedsNoPublicSelection(t *testing.T) {
	t.Parallel()

	d, ok := Describe(&Remote{
		KindValue:   domain.KindInternal,
		ReasonValue: domain.ErrReasonIndexInconsistent,
		Msg:         "index is inconsistent",
	})
	require.True(t, ok)

	require.False(t, d.PublicOverride)
	require.Equal(t, "index is inconsistent", d.Message)
}
