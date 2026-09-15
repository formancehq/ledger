package mirror

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type rejectingHTTPMirrorTransport struct {
	t *testing.T
}

func (r rejectingHTTPMirrorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Error("malformed mirror URL reached the network")

	return nil, errors.New("unexpected network request")
}

// Exercise admission, the real source and worker, proposal serialization, FSM
// application and ledger progress reads. Only the Raft transport is replaced.
func TestWorker_MalformedURLDoesNotDisclosePassword(t *testing.T) {
	t.Parallel()
	const password = "AUDIT_MIRROR_PASS_52c91"
	const ledgerName = "audit-mirror"
	const diagnostic = "parsing URL: invalid URL escape; check percent-encoding"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var logs bytes.Buffer
	log := logrus.New()
	log.SetOutput(&logs)
	log.SetFormatter(&logrus.JSONFormatter{})
	logger := logging.NewLogrus(log)
	meters := noop.NewMeterProvider()
	store, err := dal.NewStore(t.TempDir(), logger, meters.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	attrs := attributes.New()
	c, err := cache.New(100, meters.Meter("test"))
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attrs, 0)
	keys := keystore.NewKeyStore()
	shared := state.NewSharedState()
	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store, dal.NewSentinelFactory(store, false), meters, keys, shared, signal.NewNotifications(), nil, "test-cluster", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	writeMirrorMetadataPolicy(t, store, mirrorMetadataPolicy())
	require.NoError(t, state.NewRecovery(machine, store).RecoverState())
	tracker := node.NewIndexTracker(1)
	builder := plan.NewBuilder(tracker, c, attrs, store, nil, logger, 0)
	proposer := &applyingMirrorProposer{t: t, machine: machine, store: store, tracker: tracker}
	writeGate := health.NewMockWriteGate(gomock.NewController(t))
	writeGate.EXPECT().CheckWritesAllowed().Return(nil)
	adm := admission.NewAdmission(store, logger, proposer, builder, meters, writeGate, keys, shared, attrs, numscript.NewNumscriptCache(0), func(context.Context) error { return nil })
	config := &commonpb.MirrorSourceConfig{
		LedgerName: "source-ledger",
		Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{
			BaseUrl: "https://audit-user:" + password + "@localhost/%zz",
		}},
	}
	_, err = adm.Admit(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: ledgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: config}},
	}))
	require.NoError(t, err, "malformed HTTP source must reach the production worker path")
	controller := ctrl.NewDefaultController(adm, store, logger, attrs, nil, nil, meters.Meter("test"))
	ledger, err := controller.GetLedgerByName(ctx, ledgerName)
	require.NoError(t, err)
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, ledger.GetMode())
	require.Nil(t, ledger.GetMirrorSyncProgress().GetError())
	source := v2.NewHTTPSource(ledger.GetMirrorSource().GetHttp().GetBaseUrl(), config.GetLedgerName(), &http.Client{Transport: rejectingHTTPMirrorTransport{t: t}})
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	w := NewWorker(ledgerName, 100, source, nil, store, proposer, builder, logger, meters)
	proposed := 0
	proposer.beforeApply = func(cmd *raftcmdpb.Proposal) {
		proposed++
		require.Empty(t, cmd.GetOrders())
		require.Len(t, cmd.GetTechnicalUpdates(), 1)
		update := cmd.GetTechnicalUpdates()[0].GetMirrorSync()
		require.Equal(t, ledgerName, update.GetLedgerName())
		require.Contains(t, update.GetError().GetMessage(), diagnostic)
		encoded, err := cmd.MarshalVT()
		require.NoError(t, err)
		require.NotContains(t, string(encoded), password)
	}
	// Synchronous execution follows the startup loop's exact head/batch order.
	// A negative backoff makes its timer immediately ready without sleeps or
	// cancelling a successful proposal before its application is confirmed.
	w.backoff = -time.Nanosecond
	w.refreshSourceHead(ctx)
	w.processLogs(ctx)
	require.Equal(t, 1, proposed)
	require.Contains(t, logs.String(), "Failed to query source head")
	require.Contains(t, logs.String(), "Mirror sync error")
	require.Contains(t, logs.String(), ledgerName)
	require.Contains(t, logs.String(), diagnostic)
	require.NotContains(t, logs.String(), password)
	status, err := query.ReadMirrorStatus(store, ledgerName)
	require.NoError(t, err)
	require.Contains(t, status.GetMessage(), diagnostic)
	require.NotContains(t, status.GetMessage(), password)
	ledger, err = controller.GetLedgerByName(ctx, ledgerName)
	require.NoError(t, err)
	require.Equal(t, ledgerName, ledger.GetName())
	progress := ledger.GetMirrorSyncProgress()
	require.Contains(t, progress.GetError().GetMessage(), diagnostic)
	encoded, err := protojson.Marshal(progress)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), password)
	require.Zero(t, progress.GetCursor(), "failure must not advance the ingestion boundary")
}
