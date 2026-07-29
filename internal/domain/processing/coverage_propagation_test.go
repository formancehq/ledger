package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// TestLoadLedgerPropagatesCoverageMiss pins the EN-1379 contract on the highest
// leverage read site of the apply path: a coverage miss is an admission-contract
// violation and must reach the audit chain under COVERAGE_MISS, carrying the very
// Describable the Scope produced.
func TestLoadLedgerPropagatesCoverageMiss(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	miss := coverageMissDescribable{}
	expectGetLedger(mockStore, domain.LedgerKey{Name: "l-a"}, nil, miss)

	info, err := loadLedger(mockStore, "l-a")
	require.Nil(t, info)
	require.NotNil(t, err)
	require.Equal(t, domain.ErrReasonCoverageMiss, err.Reason(),
		"a coverage miss must not be relabelled as a storage fault")
	require.Equal(t, miss, err, "the violation must propagate verbatim, not re-wrapped")
}

// TestLoadLedgerWrapsGenuineStoreFault guards against over-correcting: anything
// that is not an admission-contract violation still becomes ErrStorageOperation.
func TestLoadLedgerWrapsGenuineStoreFault(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "l-a"}, nil, errors.New("pebble: db closed"))

	info, err := loadLedger(mockStore, "l-a")
	require.Nil(t, info)
	require.NotNil(t, err)
	require.Equal(t, domain.ErrReasonStorageOperation, err.Reason())
}

// TestLoadBoundariesPropagatesCoverageMiss mirrors
// TestLoadLedgerPropagatesCoverageMiss for the LedgerBoundaries channel.
func TestLoadBoundariesPropagatesCoverageMiss(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	miss := coverageMissDescribable{}
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "l-a"}, nil, miss)

	boundaries, err := loadBoundaries(mockStore, "l-a")
	require.Nil(t, boundaries)
	require.NotNil(t, err)
	require.Equal(t, domain.ErrReasonCoverageMiss, err.Reason(),
		"a coverage miss must not be relabelled as a storage fault")
	require.Equal(t, miss, err, "the violation must propagate verbatim, not re-wrapped")
}
