package admission

import (
	"bytes"
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestValidateOrder_MirrorHTTPURL(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, baseURL string
		valid         bool
	}{
		{name: "HTTP", baseURL: "http://localhost:3068", valid: true},
		{name: "HTTPS with credentials and prefix", baseURL: "https://user:password@localhost:443/prefix", valid: true},
		{name: "IPv6", baseURL: "http://[::1]:3068", valid: true},
		{name: "escaped path", baseURL: "https://localhost/a%20b", valid: true},
		{name: "empty"},
		{name: "relative", baseURL: "/prefix"},
		{name: "missing scheme", baseURL: "//localhost/prefix"},
		{name: "unsupported scheme", baseURL: "ftp://localhost/prefix"},
		{name: "missing host", baseURL: "https:///prefix"},
		{name: "empty hostname", baseURL: "https://:3068/prefix"},
		{name: "invalid escape", baseURL: "https://user:password@localhost/%zz"},
		{name: "invalid port", baseURL: "https://user:password@localhost:password"},
		{name: "control character", baseURL: "https://user:password\n@localhost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{
					Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
					MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Http{
						Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: tc.baseURL},
					}},
				}},
			}}}
			original := order.CloneVT()
			err := validateOrder(order)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrMirrorHTTPURLInvalid)
				require.NotContains(t, err.Error(), "password")
			}
			require.True(t, original.EqualVT(order), "validation must not rewrite accepted configuration")
		})
	}
}

func TestAdmission_MalformedHTTPMirrorRejectedBeforeProposal(t *testing.T) {
	t.Parallel()
	const password = "AUDIT_MIRROR_PASS_52c91"
	const ledgerName = "rejected-mirror"
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	mocks := gomock.NewController(t)
	a.proposer = NewMockProposer(mocks) // No Raft proposal is allowed.
	gate := health.NewMockWriteGate(mocks)
	gate.EXPECT().CheckWritesAllowed().Return(nil)
	a.writeGate = gate
	var logs bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&logs)
	a.logger = logging.NewLogrus(logger)
	ctx := attributedTestContext(context.Background())
	_, err := a.Admit(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_CreateLedger{
		CreateLedger: &servicepb.CreateLedgerRequest{
			Name: ledgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
			MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Http{
				Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://user:" + password + "@localhost/%zz"},
			}},
		},
	}}))
	require.ErrorIs(t, err, ErrMirrorHTTPURLInvalid)
	require.Contains(t, err.Error(), "mirrorSource.http.baseUrl")
	require.NotContains(t, err.Error(), password)
	require.NotContains(t, logs.String(), password)
	_, err = query.GetLedgerByName(ctx, store, ledgerName)
	require.ErrorIs(t, err, domain.ErrNotFound)
	status, err := query.ReadMirrorStatus(store, ledgerName)
	require.NoError(t, err)
	require.Nil(t, status)
}
