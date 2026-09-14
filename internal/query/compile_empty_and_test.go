package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// A conjunction over zero operands is vacuously true, so And{} selects the
// universe — the same rows as no filter at all — while the dual Or{} is
// vacuously false and selects nothing. Both directions are asserted: the
// descending compiler carries its own combinator, and the descending-parity
// oracle compares the two directions against each other, so a shape both get
// wrong the same way passes there.
//
// The fixture is the parity store, whose three universes are seeded wide
// enough that "equals the unfiltered scan" is a claim about rows and not an
// empty-equals-empty coincidence.
func TestCompileEmptyCombinators(t *testing.T) {
	t.Parallel()

	store := parityStore(t)

	for _, target := range []commonpb.QueryTarget{
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		commonpb.QueryTarget_QUERY_TARGET_LOGS,
	} {
		t.Run(commonpb.TargetHumanName(target), func(t *testing.T) {
			t.Parallel()

			universe := ascendingReference(t, store, target, nil)
			require.NotEmpty(t, universe)

			t.Run("empty and ascending", func(t *testing.T) {
				t.Parallel()

				require.Equal(t, universe, ascendingReference(t, store, target, andFilter()))
			})

			t.Run("empty and descending", func(t *testing.T) {
				t.Parallel()

				for _, pageSize := range []uint32{1, 5, 100} {
					require.Equal(t,
						descendingByPages(t, store, target, nil, pageSize),
						descendingByPages(t, store, target, andFilter(), pageSize))
				}
			})

			// And{} is the identity of conjunction: folding it into a
			// conjunction leaves the other operand's rows untouched, whereas an
			// empty-set reading would annihilate them.
			t.Run("empty and is the identity operand", func(t *testing.T) {
				t.Parallel()

				leaf := targetParityLeaf(t, target)

				require.Equal(t,
					ascendingReference(t, store, target, leaf),
					ascendingReference(t, store, target, andFilter(leaf, andFilter())))
			})

			t.Run("nested empty and", func(t *testing.T) {
				t.Parallel()

				require.Equal(t, universe, ascendingReference(t, store, target, andFilter(andFilter())))
			})

			t.Run("empty or selects nothing", func(t *testing.T) {
				t.Parallel()

				require.Empty(t, ascendingReference(t, store, target, orFilter()))
				require.Empty(t, descendingByPages(t, store, target, orFilter(), 5))
			})
		})
	}
}

// targetParityLeaf returns a leaf valid on target that matches some but not
// all of the fixture, so the identity case fails on a result that is neither
// the universe nor empty.
func targetParityLeaf(t *testing.T, target commonpb.QueryTarget) *commonpb.QueryFilter {
	t.Helper()

	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return stringFieldFilter("colour", "red")
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		txIDs := parityTxIDs()

		return txIDRangeFilter(txIDs[2], txIDs[len(txIDs)-3])
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		logIDs := parityLogIDs()

		return logIDRangeFilter(logIDs[2], logIDs[len(logIDs)-3])
	default:
		t.Fatalf("no parity leaf for target %v", target)

		return nil
	}
}
