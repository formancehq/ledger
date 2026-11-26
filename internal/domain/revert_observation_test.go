package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func posting(source, destination, asset, color string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Asset:       asset,
		Color:       color,
		Amount:      commonpb.NewUint256FromUint64(amount),
	}
}

func TestRevertTargetDigest_AbsentNeverCollidesWithPresent(t *testing.T) {
	t.Parallel()

	absent := RevertTargetDigest(nil, false)

	require.NotEqual(t, absent, RevertTargetDigest(nil, true),
		"an absent target and a present one with no postings are different observations")
	require.NotEqual(t, absent, RevertTargetDigest([]*commonpb.Posting{posting("world", "a", "USD", "", 1)}, true))
}

func TestRevertTargetDigest_IsDeterministic(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{
		posting("world", "users:001", "USD/2", "", 646),
		posting("users:001", "users:002", "USD/2", "GREEN", 10),
	}

	require.Equal(t, RevertTargetDigest(postings, true), RevertTargetDigest(postings, true))
	require.Equal(t, RevertTargetDigest(nil, false), RevertTargetDigest(nil, false))
}

// TestRevertTargetDigest_DistinguishesEveryField is the property the apply-time
// check rests on: any change to what admission observed must change the digest,
// or a stale view slips through and the coverage gate fires instead.
func TestRevertTargetDigest_DistinguishesEveryField(t *testing.T) {
	t.Parallel()

	base := []*commonpb.Posting{posting("world", "users:001", "USD/2", "", 646)}
	baseDigest := RevertTargetDigest(base, true)

	for _, tc := range []struct {
		name     string
		postings []*commonpb.Posting
	}{
		{"source", []*commonpb.Posting{posting("other", "users:001", "USD/2", "", 646)}},
		{"destination", []*commonpb.Posting{posting("world", "users:002", "USD/2", "", 646)}},
		{"asset", []*commonpb.Posting{posting("world", "users:001", "EUR/2", "", 646)}},
		{"color", []*commonpb.Posting{posting("world", "users:001", "USD/2", "GREEN", 646)}},
		{"amount", []*commonpb.Posting{posting("world", "users:001", "USD/2", "", 647)}},
		{"extra posting", append(append([]*commonpb.Posting{}, base...), posting("a", "b", "USD/2", "", 1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.NotEqual(t, baseDigest, RevertTargetDigest(tc.postings, true))
		})
	}
}

// TestRevertTargetDigest_OrderIsSignificant pins that reordering postings is a
// divergence: the FSM reverses them in stored order, so a different order is a
// different set of reversed postings.
func TestRevertTargetDigest_OrderIsSignificant(t *testing.T) {
	t.Parallel()

	first := posting("world", "users:001", "USD/2", "", 1)
	second := posting("users:001", "users:002", "USD/2", "", 2)

	require.NotEqual(t,
		RevertTargetDigest([]*commonpb.Posting{first, second}, true),
		RevertTargetDigest([]*commonpb.Posting{second, first}, true),
	)
}

// TestRevertTargetDigest_EncodingIsInjective pins the reason for the
// length-delimited framing. Account names, assets and colors are arbitrary
// caller bytes, so a separator-based encoding would let a crafted posting set
// serialize identically to a different one and evade detection.
func TestRevertTargetDigest_EncodingIsInjective(t *testing.T) {
	t.Parallel()

	// The same bytes, split differently across adjacent fields.
	left := []*commonpb.Posting{posting("ab", "c", "USD", "", 1)}
	right := []*commonpb.Posting{posting("a", "bc", "USD", "", 1)}

	require.NotEqual(t, RevertTargetDigest(left, true), RevertTargetDigest(right, true))
}

// TestRevertTargetDigest_EveryAmountLimbIsSignificant pins that the fixed-width
// amount block covers all four limbs, not only the low one the other cases use.
func TestRevertTargetDigest_EveryAmountLimbIsSignificant(t *testing.T) {
	t.Parallel()

	withAmount := func(amount *commonpb.Uint256) []*commonpb.Posting {
		p := posting("world", "users:001", "USD/2", "", 0)
		p.Amount = amount

		return []*commonpb.Posting{p}
	}

	zeroDigest := RevertTargetDigest(withAmount(&commonpb.Uint256{}), true)

	for _, tc := range []struct {
		name   string
		amount *commonpb.Uint256
	}{
		{"v0", &commonpb.Uint256{V0: 1}},
		{"v1", &commonpb.Uint256{V1: 1}},
		{"v2", &commonpb.Uint256{V2: 1}},
		{"v3", &commonpb.Uint256{V3: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.NotEqual(t, zeroDigest, RevertTargetDigest(withAmount(tc.amount), true))
		})
	}

	require.NotEqual(t,
		RevertTargetDigest(withAmount(&commonpb.Uint256{V0: 1}), true),
		RevertTargetDigest(withAmount(&commonpb.Uint256{V1: 1}), true),
		"the same limb value in a different position is a different amount",
	)
}

// TestRevertTargetDigest_NilAmountDigestsAsZero pins the one deliberate
// canonicalisation. Admission and apply read the target from different places,
// and nil versus an explicit zero is a representation detail of one amount, so
// distinguishing them would reject a revert whose observation did not diverge.
func TestRevertTargetDigest_NilAmountDigestsAsZero(t *testing.T) {
	t.Parallel()

	withNil := posting("world", "users:001", "USD/2", "", 0)
	withNil.Amount = nil

	withZero := posting("world", "users:001", "USD/2", "", 0)
	withZero.Amount = &commonpb.Uint256{}

	require.Equal(t,
		RevertTargetDigest([]*commonpb.Posting{withNil}, true),
		RevertTargetDigest([]*commonpb.Posting{withZero}, true),
	)
}
