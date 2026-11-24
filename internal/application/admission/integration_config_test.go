package admission

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAdmissionRejectsInvalidIntegrationConfigBeforeProposal(t *testing.T) {
	t.Parallel()

	addSink := func(config *commonpb.SinkConfig) *servicepb.Request {
		return &servicepb.Request{Type: &servicepb.Request_AddEventsSink{
			AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config},
		}}
	}
	const (
		sinkMessage   = "sink name and one sink type are required"
		mirrorMessage = "mirrorSource requires mode MIRROR"
	)
	for _, tc := range []struct {
		name    string
		request *servicepb.Request
		message string
	}{
		{"sink without config", addSink(nil), sinkMessage},
		{"named sink without type", addSink(&commonpb.SinkConfig{Name: "events"}), sinkMessage},
		{"typed sink without name", addSink(&commonpb.SinkConfig{
			Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{}},
		}), sinkMessage},
		{"normal ledger with mirror source", &servicepb.Request{Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: "normal-with-source",
				Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
				MirrorSource: &commonpb.MirrorSourceConfig{
					LedgerName: "source",
					Type: &commonpb.MirrorSourceConfig_Http{
						Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://localhost:3068"},
					},
				},
			},
		}}, mirrorMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a, _ := createTestAdmission(t, createTestStore(t))
			mocks := gomock.NewController(t)
			a.proposer = NewMockProposer(mocks) // No proposal is allowed.
			gate := health.NewMockWriteGate(mocks)
			gate.EXPECT().CheckWritesAllowed().Return(nil)
			a.writeGate = gate

			_, err := a.Admit(attributedTestContext(t.Context()), servicepb.UnsignedApplyRequest("", tc.request))
			var business *domain.BusinessError
			require.ErrorAs(t, err, &business)
			require.Equal(t, domain.KindValidation, business.Kind())
			require.Equal(t, domain.ErrReasonValidation, business.Reason())
			message, facts, overridden := domain.PublicErrorDetails(business)
			require.True(t, overridden)
			require.Equal(t, tc.message, message)
			require.Empty(t, facts)
		})
	}
}
