package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAdmitRejectsMissingAttributionBeforeDependencies(t *testing.T) {
	t.Parallel()

	// Every Admission dependency is intentionally nil. Reaching any write gate,
	// store, preload, or proposal path would panic and fail the test.
	_, err := (&Admission{}).Admit(context.Background(), &servicepb.ApplyRequest{})

	var invalid *domain.ErrInvalidCallerAttribution
	require.ErrorAs(t, err, &invalid)
}
