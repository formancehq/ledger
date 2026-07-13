package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript/numscriptmock"
)

// TestCoverageMissSurvivesNumscriptLibrary is the definitive proof for the
// EN-1406 coverage-vs-stale fix: a real *ErrCoverageMiss returned by the store
// during apply-time dependency re-resolution survives round-tripping through the
// numscript library's error path with BOTH its concrete type (errors.As) and its
// domain Reason intact. The library wraps store errors in QueryBalanceError,
// which implements Unwrap, and numscript.convertNumscriptError returns the
// underlying Describable as-is.
//
// This is why the FSM apply path can recognise a coverage-contract violation and
// surface it loudly (invariant #7) instead of masking it as retryable
// ErrStaleInputsResolution, which would spin the client in an infinite re-admit
// loop against the same missing declaration.
//
// It lives in package state because a real *ErrCoverageMiss cannot be
// constructed from the numscript/processing packages (state → processing →
// processing/numscript is an import cycle); state importing numscript is
// acyclic (numscript imports only domain).
func TestCoverageMissSurvivesNumscriptLibrary(t *testing.T) {
	t.Parallel()

	// balance() in a var origin forces a store read during resolution.
	parsed := numscriptlib.Parse(`
		vars { monetary $amt = balance(@wallet, USD/2) }
		send $amt (source = @wallet destination = @out)
	`)
	require.Empty(t, parsed.GetParsingErrors())

	// A real *ErrCoverageMiss (not a fake) returned from the generated
	// MockValueSource — the same error the coverage-gated apply Scope returns
	// when a Numscript resolution derives a key admission never declared.
	miss := &ErrCoverageMiss{Attribute: "volumes", IDHex: "deadbeef", RaftIndex: 42}

	ctrl := gomock.NewController(t)
	source := numscriptmock.NewMockValueSource(ctrl)
	source.EXPECT().Balance(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, miss)
	source.EXPECT().Metadata(gomock.Any(), gomock.Any()).AnyTimes().Return("", false, nil)

	store := numscript.NewStore(source, false)

	_, err := numscript.SafeResolveDependencies(parsed, context.Background(), numscriptlib.VariablesMap{}, store)
	require.NotNil(t, err)

	// Concrete type survives errors.As through the library's Unwrap chain.
	var got *ErrCoverageMiss
	require.ErrorAs(t, err, &got, "*ErrCoverageMiss must survive the numscript library error path")
	require.Equal(t, miss, got)

	// It must NOT be treated as a recovered panic.
	require.False(t, numscript.IsPanic(err))

	// The domain Reason the FSM apply path keys on survives too.
	require.Equal(t, domain.ErrReasonCoverageMiss, err.Reason())
}
