package check

import (
	"testing"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A scan that cannot decode a row is incomplete and must fail verification;
// it must not be reported as a healthy idempotency projection.
func TestCompareIdempotencyOutcomesRejectsIncompleteScan(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	batch := store.OpenWriteSession()
	key := make([]byte, 2+16)
	key[0] = dal.ZoneIdempotency
	key[1] = dal.SubIdempKeys
	key[len(key)-1] = 1
	require.NoError(t, batch.SetBytes(key, []byte{0xff}))
	require.NoError(t, batch.Commit())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	checker := NewChecker(store, attributes.New(), nil, logging.Testing())
	err = checker.compareIdempotencyOutcomes(handle, nil, true, func(*servicepb.CheckStoreEvent) {})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshalling idempotency value")
}
