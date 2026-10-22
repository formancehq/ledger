package grpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestLedgerInfoGRPCReadCredentialsCurrentAndCheckpoint(t *testing.T) {
	for index, source := range []*ledgerpb.MirrorSourceConfig{
		{LedgerName: "source", Type: &ledgerpb.MirrorSourceConfig_Http{Http: &ledgerpb.HttpMirrorSourceConfig{
			BaseUrl: "https://reader:base-canary@source.example/prefix", Oauth2ClientCredentials: &ledgerpb.OAuth2ClientCredentials{ClientId: "client", ClientSecret: "oauth-canary", TokenEndpoint: "https://client:token-canary@auth.example/token"},
		}}},
		{LedgerName: "source", Type: &ledgerpb.MirrorSourceConfig_Http{Http: &ledgerpb.HttpMirrorSourceConfig{
			BaseUrl:                 "http://base-token-canary@source.example:8080/prefix?region=eu&access_token=query-canary",
			Oauth2ClientCredentials: &ledgerpb.OAuth2ClientCredentials{ClientId: "client", TokenEndpoint: "http://oauth-token-canary@auth.example/token"},
		}}},
		{LedgerName: "source", Type: &ledgerpb.MirrorSourceConfig_Http{Http: &ledgerpb.HttpMirrorSourceConfig{
			BaseUrl:                 "https://base-token-canary@source.example:8080/prefix?region=eu&access_token=query-canary",
			Oauth2ClientCredentials: &ledgerpb.OAuth2ClientCredentials{ClientId: "client", TokenEndpoint: "https://oauth-token-canary@auth.example/token"},
		}}},
		{LedgerName: "source", Type: &ledgerpb.MirrorSourceConfig_Postgres{Postgres: &ledgerpb.PostgresMirrorSourceConfig{
			Dsn: "host=db.example user=reader password=postgres-canary dbname=ledger",
		}}},
	} {
		t.Run(fmt.Sprintf("source-%d", index), func(t *testing.T) {
			ledger := &ledgerpb.LedgerInfo{Name: "mirror", Mode: ledgerpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: source}
			impl := newCheckpointGateFixture(t, func(store *dal.Store) {
				batch := store.OpenWriteSession()
				require.NoError(t, state.SaveLedger(batch, "mirror", ledger))
				require.NoError(t, batch.Commit())
			})
			impl.localCtrl = ctrl.NewDefaultController(nil, impl.store, impl.logger, attributes.New(), impl.readStore, nil, noop.NewMeterProvider().Meter("test"))
			impl.ctrl = impl.localCtrl
			for _, checkpoint := range []uint64{0, gateCheckpointID} {
				t.Run(fmt.Sprintf("checkpoint-%d", checkpoint), func(t *testing.T) {
					read := &ledgerpb.ReadOptions{CheckpointId: checkpoint}
					got, err := impl.GetLedger(t.Context(), &servicepb.GetLedgerRequest{Ledger: "mirror", Read: read})
					require.NoError(t, err)
					assertSafeLedgerRead(t, got)
					stream := newFakeServerStream[ledgerpb.LedgerInfo](t)
					require.NoError(t, impl.ListLedgers(&servicepb.ListLedgersRequest{Options: &ledgerpb.ListOptions{Read: read}}, stream))
					require.Len(t, stream.sent, 1)
					assertSafeLedgerRead(t, stream.sent[0])
				})
			}
			// The same unprojected query used by internal consumers retains secrets.
			handle, err := impl.store.NewReadHandle()
			require.NoError(t, err)
			defer func() { require.NoError(t, handle.Close()) }()
			stored, err := query.GetLedgerByName(t.Context(), handle, "mirror")
			require.NoError(t, err)
			require.True(t, proto.Equal(source, stored.GetMirrorSource()))
		})
	}
}

func assertSafeLedgerRead(t *testing.T, ledger *ledgerpb.LedgerInfo) {
	t.Helper()
	data, err := proto.Marshal(ledger)
	require.NoError(t, err)
	require.NotContains(t, string(data), "canary")
	require.Equal(t, "mirror", ledger.GetName())
	require.NotNil(t, ledger.GetMirrorSyncProgress(), "mirror progress must be enriched in get and list")
	require.Equal(t, "source", ledger.GetMirrorSource().GetLedgerName())
}

func TestLedgerInfoListProgressUsesListingSnapshot(t *testing.T) {
	impl := newCheckpointGateFixture(t, func(store *dal.Store) {
		batch := store.OpenWriteSession()
		for _, name := range []string{"a", "b"} {
			require.NoError(t, state.SaveLedger(batch, name, &ledgerpb.LedgerInfo{Name: name, Mode: ledgerpb.LedgerMode_LEDGER_MODE_MIRROR}))
			require.NoError(t, state.SetMirrorSourceHead(batch, name, 10))
		}
		require.NoError(t, batch.Commit())
	})
	controller := ctrl.NewDefaultController(nil, impl.store, impl.logger, attributes.New(), impl.readStore, nil, noop.NewMeterProvider().Meter("test"))
	listing, err := controller.ListLedgers(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, listing.Close()) }()
	first, err := listing.Next()
	require.NoError(t, err)
	require.Equal(t, "a", first.GetName())
	require.Equal(t, uint64(10), first.GetMirrorSyncProgress().GetSourceLogCount())
	batch := impl.store.OpenWriteSession()
	require.NoError(t, state.SetMirrorSourceHead(batch, "b", 99))
	require.NoError(t, batch.Commit())
	second, err := listing.Next()
	require.NoError(t, err)
	require.Equal(t, "b", second.GetName())
	require.Equal(t, uint64(10), second.GetMirrorSyncProgress().GetSourceLogCount())
	handle, err := impl.store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	current, err := query.ReadMirrorSyncProgress(t.Context(), handle, attributes.New().Boundary, "b")
	require.NoError(t, err)
	require.Equal(t, uint64(99), current.GetSourceLogCount())
}
