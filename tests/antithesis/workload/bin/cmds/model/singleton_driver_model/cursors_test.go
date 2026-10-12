package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
)

func TestNextCursorLegal_PresenceFollowsTheModelsVerdict(t *testing.T) {
	t.Parallel()

	// A full page of two, so the structural half is satisfied throughout.
	const rows, pageSize = 2, 2

	// A row is waiting, so the handler's peek fired and it owes a token naming
	// the last row it actually sent.
	require.True(t, nextCursorLegal("acc:b", cursorRequired, "acc:b", rows, pageSize))
	require.False(t, nextCursorLegal("", cursorRequired, "acc:b", rows, pageSize), "a truncated page must hand back a token")
	require.False(t, nextCursorLegal("acc:c", cursorRequired, "acc:b", rows, pageSize), "the token names the last row sent, not the peeked one")

	// Nothing is waiting, so a token would promise a page that does not exist.
	require.True(t, nextCursorLegal("", cursorForbidden, "acc:b", rows, pageSize))
	require.False(t, nextCursorLegal("acc:b", cursorForbidden, "acc:b", rows, pageSize))

	// The leftover rows hinge on a stamp the model has not learned, so both
	// answers are legal — but the value still is not free.
	require.True(t, nextCursorLegal("", cursorEither, "acc:b", rows, pageSize))
	require.True(t, nextCursorLegal("acc:b", cursorEither, "acc:b", rows, pageSize))
	require.False(t, nextCursorLegal("acc:a", cursorEither, "acc:b", rows, pageSize))
}

// The peek cannot fire before a full page has been sent, so a short page carries
// no token whatever the model knows — the one direction that holds even where
// the model cannot enumerate the universe it is reading.
func TestNextCursorLegal_OnlyAFullPageCarriesAToken(t *testing.T) {
	t.Parallel()

	require.False(t, nextCursorLegal("acc:a", cursorEither, "acc:a", 1, 2), "a short page cannot have peeked")
	require.False(t, nextCursorLegal("acc:a", cursorRequired, "acc:a", 1, 2))
	require.True(t, nextCursorLegal("", cursorEither, "acc:a", 1, 2))

	// A page that showed no row has nothing a token could be derived from.
	require.False(t, nextCursorLegal("acc:a", cursorEither, "", 0, 2))
	require.True(t, nextCursorLegal("", cursorForbidden, "", 0, 2))
}

func TestMalformedCursors_AreRejectedByParseUint64(t *testing.T) {
	t.Parallel()

	// The uint64-keyed endpoints decode the token with strconv.ParseUint, so
	// every probe value must be one it refuses — a probe the server could parse
	// would assert a rejection that is not owed.
	for _, cursor := range malformedCursors {
		_, err := strconv.ParseUint(cursor, 10, 64)
		require.Error(t, err, "cursor %q must be undecodable", cursor)
	}
}

// The driver mirrors the server's page-size bounds rather than importing the
// controller into its own binary, so the mirror is pinned against the real
// clamp here — in the test binary, where linking the controller costs nothing.
func TestEffectivePageSize_MirrorsTheServerClamp(t *testing.T) {
	t.Parallel()

	require.EqualValues(t, ctrl.DefaultPageSize, serverDefaultPageSize)
	require.EqualValues(t, ctrl.MaxPageSize, serverMaxPageSize)

	for _, requested := range []int{0, 1, 2, 50, 99, 100, 999, 1000, 1001, 5000} {
		require.EqualValues(t, ctrl.ClampPageSize(uint32(requested)), effectivePageSize(requested),
			"page size %d", requested)
	}
}

// Every size the generator rolls must be one the model can predict the server's
// answer to, and the two substituted shapes must both be reachable.
func TestQueryPageSize_RollsTheSubstitutedShapes(t *testing.T) {
	t.Parallel()

	var defaulted, clamped int

	for range 4096 {
		requested, effective := queryPageSize()
		require.EqualValues(t, ctrl.ClampPageSize(uint32(requested)), effective)

		switch {
		case requested == 0:
			defaulted++
		case requested > serverMaxPageSize:
			clamped++
		}
	}

	require.Positive(t, defaulted, "the server default is never left to the server")
	require.Positive(t, clamped, "the server maximum is never exceeded")
}
