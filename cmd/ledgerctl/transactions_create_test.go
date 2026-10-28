package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type postingAmountServer struct {
	servicepb.UnimplementedBucketServiceServer

	calls    atomic.Int32
	requests chan *servicepb.ApplyRequest
}

func (s *postingAmountServer) Apply(_ context.Context, request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.calls.Add(1)
	s.requests <- request

	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}},
			}}},
		}}},
	}}}, nil
}

// Sequential: the registered root command configures process-global pterm output
// and profile environment, as in the other CLI output regression tests.
func TestTransactionsCreatePostingAmounts(t *testing.T) {
	configDir := t.TempDir()
	t.Setenv("HOME", configDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("LEDGERCTL_PROFILE", "")

	for _, color := range []string{"", "GRANTS"} {
		for _, tc := range []struct {
			name      string
			amount    string
			wantError string
		}{
			{name: "negative", amount: "-1", wantError: "amount must be between 0 and 2^256 - 1"},
			{name: "positive", amount: "1000"},
			{name: "zero", amount: "0"},
			{name: "maximum", amount: "115792089237316195423570985008687907853269984665640564039457584007913129639935"},
			{name: "overflow", amount: "115792089237316195423570985008687907853269984665640564039457584007913129639936", wantError: "amount must be between 0 and 2^256 - 1"},
		} {
			t.Run(tc.name+"/"+color, func(t *testing.T) {
				fixture := &postingAmountServer{requests: make(chan *servicepb.ApplyRequest, 2)}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				server := grpc.NewServer()
				servicepb.RegisterBucketServiceServer(server, fixture)
				serveResult := make(chan error, 1)
				go func() { serveResult <- server.Serve(listener) }()
				t.Cleanup(func() {
					server.Stop()
					if err := <-serveResult; err != nil {
						require.ErrorIs(t, err, grpc.ErrServerStopped)
					}
				})

				root := newRootCommand()
				posting := "world,alice," + tc.amount + ",USD"
				if color != "" {
					posting += "," + color
				}
				root.SetArgs([]string{
					"transactions", "create", "--ledger", "fresh",
					"--posting", "world,bob,1,USD", "--posting", posting,
					"--server", listener.Addr().String(), "--insecure", "--json",
					"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
					"--signing-key=", "--response-verify-key=", "--auth-token=test-token",
				})
				require.NotPanics(t, func() { err = root.ExecuteContext(t.Context()) })
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
					require.Zero(t, fixture.calls.Load(), "invalid input must reject the entire batch before Apply")

					return
				}
				require.NoError(t, err)
				require.EqualValues(t, 1, fixture.calls.Load())
				request := <-fixture.requests
				require.Len(t, request.GetUnsigned().GetRequests(), 1)
				apply := request.GetUnsigned().GetRequests()[0].GetApply()
				require.Equal(t, "fresh", apply.GetLedger())
				postings := apply.GetAction().GetCreateTransaction().GetPostings()
				require.Len(t, postings, 2)
				require.Equal(t, "1", postings[0].GetAmount().Dec())
				require.Equal(t, tc.amount, postings[1].GetAmount().Dec())
				require.Equal(t, "world", postings[1].GetSource())
				require.Equal(t, "alice", postings[1].GetDestination())
				require.Equal(t, "USD", postings[1].GetAsset())
				require.Equal(t, color, postings[1].GetColor())
			})
		}
	}
}
