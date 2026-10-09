//go:build sdk

package http

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestBigintOperations runs the real generated TypeScript SDK against the real
// Ledger HTTP router. Only the controller is mocked, so this covers request
// decoding, header negotiation, response encoding, and generated SDK codecs.
func TestBigintOperations(t *testing.T) {
	t.Parallel()
	script := os.Getenv("LEDGER_SDK_TEST")
	require.NotEmpty(t, script, "run through just test-sdk-bigint")
	const exact = "340282366920938463463374607431768211457"
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
			requests := req.GetUnsigned().GetRequests()
			if len(requests) != 1 {
				return nil, fmt.Errorf("SDK submitted %d requests, want 1", len(requests))
			}
			action := requests[0].GetApply().GetAction()
			if action.GetRevertTransaction() != nil {
				log := sdkLog(exact)
				log.GetPayload().GetApply().GetLog().Data = &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{
						RevertedTransactionId: 2, RevertTransaction: sdkTransaction(exact),
					}},
				}
				return &domain.ApplyResult{Logs: []*commonpb.Log{log}}, nil
			}
			request := action.GetCreateTransaction()
			if request.GetReference() == "fail" {
				return nil, &domain.ErrInsufficientFunds{Account: "world", Asset: "USD", Amount: exact, Balance: "0"}
			}
			postings := request.GetPostings()
			if len(postings) != 1 {
				return nil, fmt.Errorf("SDK submitted %d postings, want 1", len(postings))
			}
			amount := postings[0].GetAmount().Dec()
			if amount != "42" && amount != exact {
				return nil, fmt.Errorf("SDK submitted unexpected posting amount %q", amount)
			}
			return &domain.ApplyResult{Logs: []*commonpb.Log{sdkLog(amount)}}, nil
		}).Times(6)
	backend.EXPECT().GetLedgerByName(gomock.Any(), "ledger1").Return(&commonpb.LedgerInfo{Name: "ledger1"}, nil).Times(4)
	backend.EXPECT().GetTransaction(gomock.Any(), "ledger1", uint64(2)).DoAndReturn(
		func(context.Context, string, uint64) (*commonpb.Transaction, error) {
			return sdkTransaction(exact), nil
		}).Times(2)
	backend.EXPECT().GetAccount(gomock.Any(), "ledger1", gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, address string, _ ctrl.GetAccountOptions) (*commonpb.Account, error) {
			amount := exact
			if address == "numeric" {
				amount = "42"
			}
			return sdkAccount(address, amount), nil
		}).Times(2)
	backend.EXPECT().ListTransactions(gomock.Any(), "ledger1", uint32(2), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, after uint64, _ *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error) {
			first, second := sdkTransaction(exact), sdkTransaction(exact)
			first.Id, second.Id = 3, 2
			if after == 0 && !reverse {
				return cursor.NewSliceCursor([]*commonpb.Transaction{first, second}), nil
			}
			if after == 3 && !reverse {
				return cursor.NewSliceCursor([]*commonpb.Transaction{second}), nil
			}
			if after == 2 && reverse {
				return cursor.NewSliceCursor([]*commonpb.Transaction{first}), nil
			}
			return nil, fmt.Errorf("unexpected SDK transaction page after=%d reverse=%v", after, reverse)
		}).Times(3)
	backend.EXPECT().ListLogs(gomock.Any(), "ledger1", gomock.Any(), uint32(2), gomock.Any(), false).DoAndReturn(
		func(_ context.Context, _ string, after uint64, _ uint32, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Log], error) {
			first, second := sdkLog(exact), sdkLog(exact)
			second.Sequence = 8
			second.GetPayload().GetApply().GetLog().Id = 8
			if after == 0 {
				return cursor.NewSliceCursor([]*commonpb.Log{first, second}), nil
			}
			if after == 7 {
				return cursor.NewSliceCursor([]*commonpb.Log{second}), nil
			}
			return nil, fmt.Errorf("unexpected SDK ledger log page after=%d", after)
		}).Times(2)

	backend.EXPECT().GetLog(gomock.Any(), uint64(7)).Return(sdkLog(exact), nil)
	backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
			if req.GetQueryName() == "aggregate" {
				return &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Aggregate{Aggregate: sdkAggregate(exact)}}, nil
			}
			result := &commonpb.PreparedQueryCursor{PageSize: 15}
			switch req.GetQueryName() {
			case "transactions":
				result.TransactionData = []*commonpb.Transaction{sdkTransaction(exact)}
			case "accounts":
				result.AccountData = []*commonpb.Account{sdkAccount("alice", exact)}
			case "logs":
				result.LogData = []*commonpb.Log{sdkLog(exact)}
			default:
				return nil, fmt.Errorf("unexpected SDK prepared query %q", req.GetQueryName())
			}
			return &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: result}}, nil
		}).Times(4)

	backend.EXPECT().AggregateVolumes(gomock.Any(), "ledger1", gomock.Any(), gomock.Any()).Return(sdkAggregate(exact), nil)
	backend.EXPECT().ListAccounts(gomock.Any(), "ledger1", uint32(2), gomock.Any(), gomock.Any(), false).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, after string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			if after == "" {
				return cursor.NewSliceCursor([]*commonpb.Account{sdkAccount("alice", exact), sdkAccount("bob", exact)}), nil
			}
			if after == "alice" {
				return cursor.NewSliceCursor([]*commonpb.Account{sdkAccount("bob", exact)}), nil
			}
			return nil, fmt.Errorf("unexpected SDK account page after=%q", after)
		}).Times(2)

	backend.EXPECT().AnalyzeTransactions(gomock.Any(), "ledger1", gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			return &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 1, TotalReverted: 0,
				FlowPatterns: []*servicepb.FlowPattern{{Signature: "world->alice[USD]", Structure: servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE,
					TransactionCount: 1, Postings: []*servicepb.NormalizedPosting{{SourcePattern: "world", DestinationPattern: "alice", Asset: "USD"}},
					VolumeStats: []*servicepb.AssetVolumeStats{{Asset: "USD", TransactionCount: 1,
						TotalVolume: exact, AverageVolume: exact, MinVolume: exact, MaxVolume: exact,
					}},
				}},
			}, nil
		}).Times(2)
	server := httptest.NewServer(NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}))
	defer server.Close()
	command := exec.CommandContext(t.Context(), "node", script, server.URL)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func sdkTransaction(amount string) *commonpb.Transaction {
	return &commonpb.Transaction{
		PostCommitVolumes: &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{
			"alice": {Volumes: []*commonpb.VolumeEntry{{Asset: "USD", Volumes: &commonpb.Volumes{Input: commonpb.MustBigUintFromDecimal(amount), Output: commonpb.MustBigUintFromDecimal("0")}}}},
		}},
		Id: 2, Metadata: map[string]*commonpb.MetadataValue{},
		Postings: []*commonpb.Posting{{Source: "world", Destination: "alice", Asset: "USD",
			Amount: commonpb.NewUint256(uint256.MustFromDecimal(amount)),
		}},
	}
}

func sdkLog(amount string) *commonpb.Log {
	return &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{LedgerName: "ledger1", Log: &commonpb.LedgerLog{Id: 7, Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: sdkTransaction(amount)}},
		}}},
	}}}
}
func sdkAccount(address, amount string) *commonpb.Account {
	return &commonpb.Account{Address: address, Volumes: []*commonpb.AccountVolume{{Asset: "USD", Volumes: &commonpb.VolumesWithBalance{
		Input: commonpb.MustBigUintFromDecimal("0"), Output: commonpb.MustBigUintFromDecimal(amount), Balance: commonpb.MustSignedBigIntFromDecimal("-" + amount),
	}}}}
}

func sdkAggregate(amount string) *commonpb.AggregateResult {
	return &commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{{Asset: "USD",
		Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256(uint256.MustFromDecimal(amount)),
	}}}
}
