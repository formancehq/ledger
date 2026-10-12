package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// grpc.apply.duration labels every Apply exit with status. A panic in the
// controller unwinds through the handler while its error is still nil and is
// only turned into codes.Internal by the recovery interceptor afterwards, so
// a recorder that trusts err alone files the failure under status=success.
func TestApplyDurationStatus(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		ctrlApply  func(context.Context, *servicepb.ApplyRequest) (*domain.ApplyResult, error)
		wantCode   codes.Code
		wantStatus string
	}{
		{
			name: "success",
			ctrlApply: func(context.Context, *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				return &domain.ApplyResult{}, nil
			},
			wantCode:   codes.OK,
			wantStatus: "success",
		},
		{
			name: "error",
			ctrlApply: func(context.Context, *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				return nil, status.Error(codes.FailedPrecondition, "rejected")
			},
			wantCode:   codes.FailedPrecondition,
			wantStatus: "error",
		},
		{
			name: "recovered panic",
			ctrlApply: func(context.Context, *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				panic(errors.New("controller exploded"))
			},
			wantCode:   codes.Internal,
			wantStatus: "error",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
			applyDuration, err := meter.Float64Histogram("grpc.apply.duration")
			require.NoError(t, err)

			mockCtrl := ctrlmock.NewMockController(gomock.NewController(t))
			mockCtrl.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(test.ctrlApply)
			impl := &BucketServiceServerImpl{logger: noopLogger{}, ctrl: mockCtrl, applyDuration: applyDuration}

			// The production chain: the recovery interceptor wraps the handler.
			intercept := recoveryInterceptor(logging.FromContext(logging.TestingContext()))
			_, err = intercept(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{}),
				&ggrpc.UnaryServerInfo{FullMethod: "/test/Apply"},
				func(ctx context.Context, req any) (any, error) {
					return impl.Apply(ctx, req.(*servicepb.ApplyRequest))
				})
			require.Equal(t, test.wantCode, status.Code(err), "client-visible code")

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))
			counts := map[string]uint64{}
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					histogram, ok := m.Data.(metricdata.Histogram[float64])
					require.True(t, ok, m.Name)
					for _, dp := range histogram.DataPoints {
						value, _ := dp.Attributes.Value("status")
						counts[value.AsString()] += dp.Count
					}
				}
			}
			require.Equal(t, map[string]uint64{test.wantStatus: 1}, counts,
				"exactly one observation, labelled %s", test.wantStatus)
		})
	}
}
