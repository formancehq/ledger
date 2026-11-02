package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func postgresURLOrder(raw string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{
			Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
			MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: raw},
			}},
		}},
	}}}
}

func sinkURLOrder(cfg *commonpb.SinkConfig) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: cfg}},
	}}}
}

func TestValidateOrderConnectionURLs(t *testing.T) {
	t.Parallel()
	postgres := postgresURLOrder
	nats := func(raw string) *raftcmdpb.Order {
		return sinkURLOrder(&commonpb.SinkConfig{Name: "events", Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{Url: raw, Topic: "events"}}})
	}
	clickhouse := func(raw string) *raftcmdpb.Order {
		return sinkURLOrder(&commonpb.SinkConfig{Name: "events", Type: &commonpb.SinkConfig_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfig{Dsn: raw}}})
	}
	for _, tc := range []struct {
		name, raw string
		order     func(string) *raftcmdpb.Order
		invalid   error
	}{
		{"PostgreSQL URL", "postgres://alice:canary@db/ledger?sslmode=require", postgres, nil},
		{"PostgreSQL long scheme", "postgresql://alice@db/ledger", postgres, nil},
		{"PostgreSQL socket", "postgresql:///ledger?host=%2Ftmp", postgres, nil},
		{"PostgreSQL keyword", "host=db password=canary", postgres, ErrMirrorPostgresURLInvalid},
		{"PostgreSQL empty", "", postgres, ErrMirrorPostgresURLInvalid},
		{"PostgreSQL opaque", "postgres:password=canary", postgres, ErrMirrorPostgresURLInvalid},
		{"PostgreSQL missing slashes", "postgres:/ledger", postgres, ErrMirrorPostgresURLInvalid},
		{"PostgreSQL cross format", "https://canary@db", postgres, ErrMirrorPostgresURLInvalid},
		{"PostgreSQL uppercase unsupported by pgx", "POSTGRES://db/ledger", postgres, ErrMirrorPostgresURLInvalid},
		{"PostgreSQL malformed", "postgres://alice:canary@db/%zz", postgres, ErrMirrorPostgresURLInvalid},
		{"NATS list", "nats://canary@one:4222, tls://alice:canary@two:4222,ws://three:80,wss://four:443", nats, nil},
		{"NATS IPv6", "nats://[::1]:4222", nats, nil},
		{"NATS empty", "", nats, ErrSinkNATSURLInvalid},
		{"NATS bare host", "canary@one:4222", nats, ErrSinkNATSURLInvalid},
		{"NATS second bare host", "nats://one,two:4222", nats, ErrSinkNATSURLInvalid},
		{"NATS empty member", "nats://one,", nats, ErrSinkNATSURLInvalid},
		{"NATS cross format", "http://canary@one", nats, ErrSinkNATSURLInvalid},
		{"NATS empty hostname", "nats://:4222", nats, ErrSinkNATSURLInvalid},
		{"NATS malformed", "nats://alice:canary@one:bad", nats, ErrSinkNATSURLInvalid},
		{"ClickHouse native multi host", "clickhouse://alice:canary@one:9000,two:9000/ledger?compress=true", clickhouse, nil},
		{"ClickHouse HTTP proxy", "https://alice:canary@db/ledger?http_proxy=https%3A%2F%2Fcanary%40proxy", clickhouse, nil},
		{"ClickHouse HTTP", "http://db/ledger", clickhouse, nil},
		{"ClickHouse empty", "", clickhouse, ErrSinkClickHouseURLInvalid},
		{"ClickHouse bare host", "db:9000", clickhouse, ErrSinkClickHouseURLInvalid},
		{"ClickHouse cross format", "postgres://canary@db", clickhouse, ErrSinkClickHouseURLInvalid},
		{"ClickHouse opaque", "clickhouse:canary", clickhouse, ErrSinkClickHouseURLInvalid},
		{"ClickHouse missing host", "clickhouse:///ledger", clickhouse, ErrSinkClickHouseURLInvalid},
		{"ClickHouse malformed", "clickhouse://alice:canary@db/%zz", clickhouse, ErrSinkClickHouseURLInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			order := tc.order(tc.raw)
			original := order.CloneVT()
			err := validateOrder(order)
			if tc.invalid == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.invalid)
				require.NotContains(t, err.Error(), "canary")
			}
			require.True(t, original.EqualVT(order), "validation must not rewrite configuration")
		})
	}
}

func TestAdmissionRejectsNonURLConnectionsBeforeProposal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		request *servicepb.Request
		invalid error
	}{
		{"PostgreSQL", &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
			Name: "mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
			MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "host=db password=canary"}}},
		}}}, ErrMirrorPostgresURLInvalid},
		{"NATS", &servicepb.Request{Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{Config: &commonpb.SinkConfig{
			Name: "events", Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{Url: "nats://one,canary@two", Topic: "events"}},
		}}}}, ErrSinkNATSURLInvalid},
		{"ClickHouse", &servicepb.Request{Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{Config: &commonpb.SinkConfig{
			Name: "events", Type: &commonpb.SinkConfig_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfig{Dsn: "host=db password=canary"}},
		}}}}, ErrSinkClickHouseURLInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, _ := createTestAdmission(t, createTestStore(t))
			mocks := gomock.NewController(t)
			a.proposer = NewMockProposer(mocks) // No proposal is allowed.
			gate := health.NewMockWriteGate(mocks)
			gate.EXPECT().CheckWritesAllowed().Return(nil)
			a.writeGate = gate
			_, err := a.Admit(attributedTestContext(context.Background()), servicepb.UnsignedApplyRequest("", tc.request))
			require.ErrorIs(t, err, tc.invalid)
			var business *domain.BusinessError
			require.ErrorAs(t, err, &business)
			message, facts, public := domain.PublicErrorDetails(business)
			require.True(t, public)
			require.Equal(t, domain.KindValidation, business.Kind())
			require.Equal(t, domain.ErrReasonValidation, business.Reason())
			require.Equal(t, tc.invalid.Error(), message)
			require.Empty(t, facts)
			require.NotContains(t, err.Error(), "canary")
		})
	}
}
