package mirror

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
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

// Admission rejects the malformed URL. Seed the persisted fixture separately
// to retain runtime defense coverage for configurations loaded outside admission.
// The source, worker, FSM and reads are real; only Raft transport is replaced.
func TestWorker_MalformedURLDoesNotDisclosePassword(t *testing.T) {
	t.Parallel()
	const password = "AUDIT_MIRROR_PASS_52c91"
	const ledgerName = "audit-mirror"
	const diagnostic = "parsing URL: invalid mirror source URL"
	ctx, cancel := context.WithTimeout(internalauth.WithSystemActor(context.Background(), commands.ComponentMirror), 10*time.Second)
	defer cancel()
	var logs bytes.Buffer
	log := logrus.New()
	log.SetOutput(&logs)
	log.SetFormatter(&logrus.JSONFormatter{})
	logger := logging.NewLogrus(log)
	meters := noop.NewMeterProvider()
	store, err := dal.NewStore(t.TempDir(), logger, meters.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	seedMirrorAuditKey(t, store)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	attrs := attributes.New()
	c, err := cache.New(100, meters.Meter("test"))
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attrs)
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
	require.ErrorIs(t, err, admission.ErrMirrorHTTPURLInvalid)
	require.Equal(t, uint64(1), tracker.Next(), "rejection must not propose to Raft")
	key := domain.LedgerKey{Name: ledgerName}
	info := &commonpb.LedgerInfo{Name: ledgerName, Id: 1, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: config}
	session := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(session, ledgerName, info))
	_, _, err = registry.Ledgers.PutWithCache(session, 0, key.Bytes(), info)
	require.NoError(t, err)
	_, _, err = registry.Boundaries.PutWithCache(session, 0, key.Bytes(), &raftcmdpb.LedgerBoundaries{NextTransactionId: 1})
	require.NoError(t, err)
	require.NoError(t, session.Commit())
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

// The client adds userinfo authentication before the transport runs. Return the
// request URL as a transport might, exercising sanitization of both net/http's
// outer url.Error and its underlying diagnostic without contacting a network.
type failingHTTPMirrorTransport struct {
	t            *testing.T
	user         *url.Userinfo
	calls        []string
	redirectUser *url.Userinfo
}

func (r *failingHTTPMirrorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.t.Helper()
	if r.redirectUser != nil && req.URL.Host == "source.invalid" {
		r.calls = append(r.calls, req.URL.Query().Get("sort"))

		return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://" + r.redirectUser.String() + "@redirect.invalid/final"}}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
	}
	expectedUser := r.user
	if r.redirectUser != nil {
		expectedUser = r.redirectUser
	}
	username, password, ok := req.BasicAuth()
	expectedPassword, _ := expectedUser.Password()
	require.True(r.t, ok)
	require.Equal(r.t, expectedUser.Username(), username)
	require.Equal(r.t, expectedPassword, password)
	require.Equal(r.t, expectedUser.String(), req.URL.User.String())
	if r.redirectUser == nil {
		r.calls = append(r.calls, req.URL.Query().Get("sort"))
	}

	return nil, fmt.Errorf("mirror transport sentinel: %s", req.URL.String())
}

func TestWorker_TransportErrorsDoNotDiscloseCredentials(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		scheme string
		user   *url.Userinfo
	}{
		{name: "replacement marker", scheme: "https", user: url.User("[redacted]")},
		{name: "http passwordless token", scheme: "http", user: url.User("WORKER_TOKEN_91a7")},
		{name: "https escaped passwordless token", scheme: "https", user: url.User("WORKER/TOKEN?91a7")},
		{name: "http username and password", scheme: "http", user: url.UserPassword("WORKER_USER_91a7", "WORKER_PASS_91a7")},
		{name: "https escaped username and password", scheme: "https", user: url.UserPassword("WORKER/USER?91a7", "WORKER/PASS?91a7")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, redirect := range []bool{false, true} {
				t.Run(fmt.Sprintf("redirect=%t", redirect), func(t *testing.T) {
					t.Parallel()
					const ledgerName = "transport-mirror"
					const diagnostic = "mirror transport sentinel"
					ctx, cancel := context.WithTimeout(internalauth.WithSystemActor(context.Background(), commands.ComponentMirror), 10*time.Second)
					defer cancel()
					var logs bytes.Buffer
					log := logrus.New()
					log.SetOutput(&logs)
					log.SetFormatter(&logrus.JSONFormatter{})
					logger := logging.NewLogrus(log)
					meters := noop.NewMeterProvider()
					store, err := dal.NewStore(t.TempDir(), logger, meters.Meter("test"), dal.DefaultConfig())
					require.NoError(t, err)
					seedMirrorAuditKey(t, store)
					t.Cleanup(func() { require.NoError(t, store.Close()) })
					attrs := attributes.New()
					c, err := cache.New(100, meters.Meter("test"))
					require.NoError(t, err)
					registry := state.NewStateRegistry(c, attrs)
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

					sourceUser := tc.user
					if redirect {
						sourceUser = url.User("INITIAL_WORKER_TOKEN")
					}
					baseURL := (&url.URL{Scheme: tc.scheme, Host: "source.invalid", User: sourceUser}).String()
					config := &commonpb.MirrorSourceConfig{
						LedgerName: "source-ledger",
						Type:       &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: baseURL}},
					}
					_, err = adm.Admit(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
						Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: ledgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: config}},
					}))
					require.NoError(t, err)
					controller := ctrl.NewDefaultController(adm, store, logger, attrs, nil, nil, meters.Meter("test"))
					ledger, err := controller.GetLedgerByName(ctx, ledgerName)
					require.NoError(t, err)
					require.Equal(t, baseURL, ledger.GetMirrorSource().GetHttp().GetBaseUrl())
					require.Nil(t, ledger.GetMirrorSyncProgress().GetError())
					transport := &failingHTTPMirrorTransport{t: t, user: sourceUser}
					if redirect {
						transport.redirectUser = tc.user
					}
					source := v2.NewHTTPSource(baseURL, config.GetLedgerName(), &http.Client{Transport: transport})
					t.Cleanup(func() { require.NoError(t, source.Close()) })
					w := NewWorker(ledgerName, 100, source, nil, store, proposer, builder, logger, meters)
					assertSafe := func(diagnosticText string) {
						t.Helper()
						password, hasPassword := tc.user.Password()
						secrets := []string{sourceUser.Username(), tc.user.Username(), url.User(tc.user.Username()).String(), tc.user.String()}
						if hasPassword {
							secrets = append(secrets, password, url.User(password).String())
						}
						for _, secret := range secrets {
							require.NotContains(t, diagnosticText, secret)
						}
					}
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
						assertSafe(string(encoded))
					}
					// Follow the startup loop synchronously with an immediately ready backoff.
					w.backoff = -time.Nanosecond
					w.refreshSourceHead(ctx)
					w.processLogs(ctx)
					require.Equal(t, []string{"id:desc", "id:asc"}, transport.calls)
					require.Equal(t, 1, proposed)
					// Inspect each structured log independently so both request paths must retain
					// their operation, ledger identity and harmless transport diagnostic.
					decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
					observed := map[string]bool{}
					for decoder.More() {
						var entry map[string]any
						require.NoError(t, decoder.Decode(&entry))
						message, _ := entry["msg"].(string)
						if message != "Failed to query source head" && message != "Mirror sync error" {
							continue
						}
						observed[message] = true
						require.Equal(t, ledgerName, entry["ledger"])
						require.Contains(t, entry["error"], diagnostic)
						assertSafe(fmt.Sprint(entry["error"]))
					}
					require.True(t, observed["Failed to query source head"])
					require.True(t, observed["Mirror sync error"])
					assertSafe(logs.String())
					status, err := query.ReadMirrorStatus(store, ledgerName)
					require.NoError(t, err)
					require.Contains(t, status.GetMessage(), diagnostic)
					assertSafe(status.GetMessage())
					ledger, err = controller.GetLedgerByName(ctx, ledgerName)
					require.NoError(t, err)
					require.Equal(t, baseURL, ledger.GetMirrorSource().GetHttp().GetBaseUrl(), "diagnostics must not rewrite persisted authentication")
					progress := ledger.GetMirrorSyncProgress()
					require.Contains(t, progress.GetError().GetMessage(), diagnostic)
					encoded, err := protojson.Marshal(progress)
					require.NoError(t, err)
					assertSafe(string(encoded))
					require.Zero(t, progress.GetCursor(), "failure must not advance the ingestion boundary")
				})
			}
		})
	}
}
