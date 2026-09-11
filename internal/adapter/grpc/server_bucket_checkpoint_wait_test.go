package grpc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// EN-1954 moved the query-checkpoint readiness wait out of the removed
// ClusterService.CreateQueryCheckpoint handler and into the common Apply
// response path. Two properties are easy to break and expensive to lose:
//
//   - the wait must find the checkpoint log anywhere in the batch. The old
//     handler read logs[0], which was only correct because its batch always
//     held exactly one request. Through Apply the checkpoint trigger is the
//     LAST action of the batch.
//   - the wait must run BEFORE response payload stripping. skip_response nils
//     out the payload carrying the checkpoint id, so a wait placed after it
//     silently degrades to no wait at all — reintroducing the EN-1460 race
//     with no failing test.
func TestApplyWaitsForCreatedQueryCheckpoint(t *testing.T) {
	t.Parallel()

	preparedQueryLog := func() *commonpb.Log {
		return &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreatedPreparedQuery{
				CreatedPreparedQuery: &commonpb.CreatedPreparedQueryLog{},
			},
		}}
	}
	checkpointLog := func(id uint64) *commonpb.Log {
		return &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
				CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: id, MaxSequence: 7},
			},
		}}
	}

	tests := []struct {
		name         string
		logs         []*commonpb.Log
		skipResponse bool
		markReady    []uint64 // checkpoint IDs materialized before Apply runs
		wantWait     bool
	}{
		{
			name:     "batch without a checkpoint does not wait",
			logs:     []*commonpb.Log{preparedQueryLog(), preparedQueryLog()},
			wantWait: false,
		},
		{
			name:      "already materialized checkpoint returns immediately",
			logs:      []*commonpb.Log{checkpointLog(1)},
			markReady: []uint64{1},
			wantWait:  false,
		},
		{
			name:     "checkpoint as the last action of a mixed batch is found",
			logs:     []*commonpb.Log{preparedQueryLog(), preparedQueryLog(), checkpointLog(2)},
			wantWait: true,
		},
		{
			name:         "skip_response still waits, because the wait precedes stripping",
			logs:         []*commonpb.Log{preparedQueryLog(), checkpointLog(3)},
			skipResponse: true,
			wantWait:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			impl, mockCtrl := newCheckpointWaitHarness(t)

			for _, id := range test.markReady {
				dir := impl.store.QueryCheckpointReadIndexDir(id)
				require.NoError(t, readstore.MarkCheckpointReady(mkdirAllForCheckpoint(t, dir)))
			}

			mockCtrl.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: test.logs}, nil)

			// A deadline is the only observable difference between "waited" and
			// "did not wait": an unmaterialized checkpoint parks until the
			// context expires, anything else returns well inside it.
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			// A caller cannot bypass fresh-creation readiness by spoofing provenance.
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(metadataKeyApplyReplayed, "true"))

			req := servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
				},
			})
			req.SkipResponse = test.skipResponse

			resp, err := impl.Apply(ctx, req)

			if test.wantWait {
				require.ErrorContains(t, err, "waiting for read index checkpoint",
					"Apply must block on the local read-index marker before returning")

				return
			}

			require.NoError(t, err)
			require.Len(t, resp.GetLogs(), len(test.logs))
		})
	}
}

func newCheckpointWaitHarness(t *testing.T) (*BucketServiceServerImpl, *ctrlmock.MockController) {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	mainStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mainStore.Close() })

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	applyDuration, err := meter.Int64Histogram("test.apply.duration")
	require.NoError(t, err)

	mockCtrl := ctrlmock.NewMockController(gomock.NewController(t))

	return &BucketServiceServerImpl{
		logger:        noopLogger{},
		ctrl:          mockCtrl,
		store:         mainStore,
		readStore:     rs,
		authCfg:       internalauth.AuthConfig{}, // disabled → Authenticate is a no-op
		applyDuration: applyDuration,
	}, mockCtrl
}

func mkdirAllForCheckpoint(t *testing.T, dir string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o750))

	return dir
}
