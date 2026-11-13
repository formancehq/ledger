package ctrl

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/incarnationmigration"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAccountDatesComeFromIncarnation(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	attrs := attributes.New()
	const ledger = "dates"
	const address = "users:alice"
	accountKey := domain.AccountKey{LedgerName: ledger, Account: address}

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
	_, err = attrs.AccountIncarnation.Set(batch, accountKey.Bytes(), &commonpb.AccountIncarnation{
		InsertedAt: &commonpb.Timestamp{Data: 100},
		UpdatedAt:  &commonpb.Timestamp{Data: 200},
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}}}
	batch = store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, policy))
	require.NoError(t, batch.Commit())
	require.NoError(t, incarnationmigration.Run(t.Context(), store))
	policy.CapabilityActivations = append(policy.CapabilityActivations, &commonpb.CapabilityActivation{Id: uint32(capabilities.AccountIncarnationHistoryCompleteV1), Active: true})
	batch = store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, policy))
	require.NoError(t, batch.Commit())

	controller := NewDefaultController(nil, store, logger, attrs, nil, nil, meter)
	got, err := controller.GetAccount(t.Context(), ledger, address, GetAccountOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), got.GetInsertionDate().GetData())
	require.Equal(t, uint64(200), got.GetUpdatedAt().GetData())
	require.Nil(t, got.GetFirstUsage())
	body, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(body), `"insertionDate":`)
	require.Contains(t, string(body), `"updatedAt":`)

	// ListAccounts and prepared account queries use this same enrichment path.
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	listed, err := query.EnrichAccounts([][]byte{[]byte(address)}, controller.entityEnricher(), handle, ledger)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, got.GetInsertionDate().GetData(), listed[0].GetInsertionDate().GetData())
	require.Equal(t, got.GetUpdatedAt().GetData(), listed[0].GetUpdatedAt().GetData())

	batch = store.OpenWriteSession()
	_, err = attrs.AccountIncarnation.Set(batch, accountKey.Bytes(), &commonpb.AccountIncarnation{
		InsertedAt: &commonpb.Timestamp{Data: 100},
		UpdatedAt:  &commonpb.Timestamp{Data: 300},
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// The listing's existing read handle keeps all account fields at one snapshot.
	frozen, err := query.EnrichAccounts([][]byte{[]byte(address)}, controller.entityEnricher(), handle, ledger)
	require.NoError(t, err)
	require.Equal(t, uint64(200), frozen[0].GetUpdatedAt().GetData())
	latest, err := controller.GetAccount(t.Context(), ledger, address, GetAccountOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), latest.GetInsertionDate().GetData())
	require.Equal(t, uint64(300), latest.GetUpdatedAt().GetData())
}

func TestAccountDatesAbsentWithoutIncarnation(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	got, err := scanAccount(handle, attributes.New(), "ledger", "users:alice", false, true)
	require.NoError(t, err)
	require.Nil(t, got.GetInsertionDate())
	require.Nil(t, got.GetUpdatedAt())
}

func TestAccountDatesRequireLocalMigrationMarker(t *testing.T) {
	store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, &commonpb.ClusterPolicy{
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}, {Id: uint32(capabilities.AccountIncarnationHistoryCompleteV1), Active: true}},
	}))
	require.NoError(t, batch.Commit())
	active, err := accountDatesActive(store)
	require.False(t, active)
	require.ErrorContains(t, err, "without a completed local migration")
}
