package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestErrAggregateOverflow_Describable(t *testing.T) {
	t.Parallel()

	err := &ErrAggregateOverflow{Stage: "collapse-colors", Side: "input"}

	require.Equal(t, "aggregate volume input overflowed 2^256 during collapse-colors", err.Error())
	require.Equal(t, domain.ErrReasonAggregateOverflow, err.Reason())
	require.Equal(t, map[string]string{"stage": "collapse-colors", "side": "input"}, err.Metadata())

	// The reason must classify as a precondition failure through the shared
	// domain kind switch, exactly as it did when the type lived in domain.
	require.Equal(t, domain.KindPrecondition, domain.Kind(err))
}
