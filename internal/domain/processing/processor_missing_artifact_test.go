package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// missingArtifactCount returns the numscript.artifact.missing total the reader
// collected, or 0 when the counter was never incremented.
func missingArtifactCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "numscript.artifact.missing" {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)

			for _, point := range sum.DataPoints {
				total += point.Value
			}
		}
	}

	return total
}

func numscriptWorldSendRequest(script string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: "test-ledger",
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
			CreateTransaction: &servicepb.CreateTransactionPayload{Script: &commonpb.Script{Plain: script}},
		}},
	}}}
}

// TestProcessCreateTransaction_Numscript_MissingArtifactRecompiles pins that a
// scripted order reaching apply without its compiled artifact is recompiled
// from the script text and applied with the same outcome it would have had
// with one. On the cluster's processor the alarm counter records the admission
// bug; on the audit replay's processor, where missing artifacts are expected,
// it stays silent.
func TestProcessCreateTransaction_Numscript_MissingArtifactRecompiles(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		auditReplay bool
		wantMissing int64
	}{
		{name: "cluster processor raises the alarm", auditReplay: false, wantMissing: 1},
		{name: "audit replay stays silent", auditReplay: true, wantMissing: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			processor, err := NewRequestProcessor(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), 0)
			require.NoError(t, err)
			if tc.auditReplay {
				processor.ExpectMissingNumscriptArtifacts()
			}

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			expectDefaultMetadataLimits(mockStore)
			expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}).AsReader(), nil)
			expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
			mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1234567890}).AsReader()).AnyTimes()
			expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
			setupNumscriptVolumeMocks(mockStore)
			mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
			expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, nil)

			order := requestToOrderWithoutArtifact(numscriptWorldSendRequest(
				`send [USD/2 10000] (source = @world destination = @users:alice)`))
			require.Empty(t, order.GetTechnical().GetCompiledProgram())

			result, procErr := processor.ProcessOrder(order, mockStore)
			require.Nil(t, procErr)

			postings := result.GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetPostings()
			require.Len(t, postings, 1)
			require.Equal(t, "world", postings[0].GetSource())
			require.Equal(t, "users:alice", postings[0].GetDestination())
			require.Equal(t, int64(10000), postings[0].GetAmount().ToBigInt().Int64())
			require.Equal(t, "USD/2", postings[0].GetAsset())

			require.Equal(t, tc.wantMissing, missingArtifactCount(t, reader))
		})
	}
}

// TestProcessCreateTransaction_Numscript_MissingArtifactCompileError pins that
// recompiling a missing artifact surfaces the compiler's own rejection — the
// error admission would have returned — rather than a generic runtime error.
func TestProcessCreateTransaction_Numscript_MissingArtifactCompileError(t *testing.T) {
	t.Parallel()

	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}).AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	setupNumscriptVolumeMocks(mockStore)

	order := requestToOrderWithoutArtifact(numscriptWorldSendRequest(
		`send [USD/2 *] (source = @sa:credit allowing unbounded overdraft destination = @users:alice)`))

	result, procErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, result)

	var compileErr *domain.ErrNumscriptCompile
	require.ErrorAs(t, procErr, &compileErr)
}
