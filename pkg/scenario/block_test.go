package scenario

import (
	"context"
	"io"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// typedDraws deliberately separates bounded draws from raw entropy. A block
// that derives a choice from Uint64 instead of the typed API fails these tests.
type typedDraws struct {
	ints   []int
	int64s []int64
	raw    int
	upper  bool
}

func (r *typedDraws) Intn(n int) int {
	if n <= 0 {
		panic("non-positive bound")
	}
	r.ints = append(r.ints, n)
	if r.upper {
		return n - 1
	}

	return 0
}
func (r *typedDraws) Int63n(n int64) int64 {
	if n <= 0 {
		panic("non-positive bound")
	}
	r.int64s = append(r.int64s, n)
	if r.upper {
		return n - 1
	}

	return 0
}
func (r *typedDraws) Uint64() uint64 {
	r.raw++

	return 42
}

type blockClient struct {
	servicepb.BucketServiceClient

	balance  int64
	requests []*servicepb.ApplyRequest
}

func (c *blockClient) Apply(_ context.Context, req *servicepb.ApplyRequest, _ ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	c.requests = append(c.requests, req)

	return &servicepb.ApplyResponse{}, nil
}
func (c *blockClient) GetAccount(context.Context, *servicepb.GetAccountRequest, ...grpc.CallOption) (*commonpb.Account, error) {
	var volumes []*commonpb.AccountVolume
	for _, asset := range []string{"USD/2", "EUR/2", "GBP/2", "COINS"} {
		volumes = append(volumes, &commonpb.AccountVolume{Asset: asset, Volumes: &commonpb.VolumesWithBalance{Balance: commonpb.NewSignedBigInt(big.NewInt(c.balance))}})
	}

	return &commonpb.Account{Volumes: volumes}, nil
}

type transactions struct {
	grpc.ServerStreamingClient[commonpb.Transaction]

	next int
}

func (s *transactions) Recv() (*commonpb.Transaction, error) {
	s.next++
	switch s.next {
	case 1:
		return &commonpb.Transaction{Id: 11, Reverted: true}, nil
	case 2, 3:
		return &commonpb.Transaction{Id: uint64(s.next)}, nil
	default:
		return nil, io.EOF
	}
}
func (*blockClient) ListTransactions(context.Context, *servicepb.ListTransactionsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Transaction], error) {
	return &transactions{}, nil
}

func TestBlockTypedDrawsAndRegistry(t *testing.T) {
	t.Parallel()
	groups := []*BlockGroup{MarketplaceBlocks(), GamingWalletBlocks(), LendingLifecycleBlocks(), SubscriptionBlocks(), MultiLedgerPayrollBlocks(), MultiCurrencyBlocks()}
	expected := []struct {
		name    string
		ints    []int
		int64s  []int64
		raw     int
		applies int
	}{
		{"marketplace/deposit", []int{50}, []int64{1000000}, 0, 1},
		{"marketplace/purchase", []int{50, 10}, []int64{49001}, 0, 1},
		{"marketplace/revert", []int{2}, nil, 0, 1},
		{"marketplace/payout", []int{10}, nil, 0, 1},
		{"marketplace/metadata", []int{2, 50, 5, 3}, nil, 1, 2},
		{"gaming/topup", []int{20, 10000}, nil, 0, 1},
		{"gaming/buy_item", []int{20, 3, 100}, nil, 0, 1},
		{"gaming/p2p_trade", []int{20, 20}, []int64{99999991}, 0, 1},
		{"gaming/promotion", []int{20, 1000}, nil, 0, 1},
		{"gaming/clawback", []int{20}, []int64{99999951}, 0, 1},
		{"lending/fund_pool", nil, []int64{5}, 0, 1},
		{"lending/disburse", []int{10}, nil, 0, 1},
		{"lending/repay", []int{10}, nil, 0, 1},
		{"lending/provision", []int{10}, nil, 0, 1},
		{"lending/write_off", []int{10}, nil, 0, 1},
		{"subscription/fund", []int{50, 3}, nil, 0, 1},
		{"subscription/charge", []int{50}, nil, 0, 1},
		{"subscription/recognize", []int{76}, nil, 0, 1},
		{"subscription/credit", []int{50, 500}, nil, 0, 1},
		{"payroll/fund_clearing", nil, nil, 0, 1},
		{"payroll/distribute", []int{3}, nil, 0, 1},
		{"payroll/pay_salary", []int{3, 15}, nil, 0, 1},
		{"payroll/cost_alloc", []int{3}, nil, 0, 1},
		{"multicurrency/fund", []int{3, 900000}, nil, 0, 1},
		{"multicurrency/fx", []int{3, 2}, []int64{99995001}, 0, 2},
		{"multicurrency/vendor_pay", []int{3, 6}, []int64{10000}, 0, 1},
	}
	var blocks []*Block
	for _, group := range groups {
		require.NotNil(t, group.Setup)
		require.NotEmpty(t, group.Setup())
		blocks = append(blocks, group.Blocks...)
	}
	require.Len(t, blocks, 26)
	for i, b := range blocks {
		want := expected[i]
		require.Equal(t, want.name, b.Name)
		t.Run(b.Name, func(t *testing.T) {
			t.Parallel()
			r := &typedDraws{}
			client := &blockClient{balance: 100000000}
			_, err := b.Run(context.Background(), client, r)
			require.NoError(t, err)
			require.Equal(t, want.ints, r.ints)
			require.Equal(t, want.int64s, r.int64s)
			require.Equal(t, want.raw, r.raw)
			require.Len(t, client.requests, want.applies)
		})
	}
}

func TestBlockPreconditions(t *testing.T) {
	t.Parallel()
	for _, run := range []struct {
		name string
		run  func(context.Context, servicepb.BucketServiceClient, Random) (*servicepb.ApplyResponse, error)
	}{
		{"purchase", marketplacePurchase}, {"payout", marketplacePayout},
		{"buy_item", gamingBuyItem}, {"p2p_trade", gamingP2PTrade}, {"clawback", gamingClawback},
		{"disburse", lendingDisburse}, {"repay", lendingRepay}, {"provision", lendingProvision}, {"write_off", lendingWriteOff},
		{"charge", subscriptionCharge}, {"recognize", subscriptionRecognize},
		{"distribute", payrollDistribute}, {"cost_alloc", payrollCostAlloc},
		{"fx", multiCurrencyFX}, {"vendor_pay", multiCurrencyVendorPay},
	} {
		t.Run(run.name, func(t *testing.T) {
			t.Parallel()
			client := &blockClient{}
			_, err := run.run(context.Background(), client, &typedDraws{})
			require.ErrorIs(t, err, ErrSkip)
			require.Empty(t, client.requests)
		})
	}
}

func TestRandFuncAdapter(t *testing.T) {
	t.Parallel()
	// Preserve the existing modulo mapping for legacy callers.
	r := RandFunc(func() uint64 { return math.MaxUint64 })
	require.Equal(t, uint64(math.MaxUint64), r.Uint64())
	require.Equal(t, 5, RandIntN(r, 10))
	require.Equal(t, int64(1), RandInt64N(r, math.MaxInt64))
	require.Panics(t, func() { r.Intn(0) })
	require.Panics(t, func() { r.Int63n(-1) })
	// Existing callers may still pass a RandFunc directly to Block.Run.
	_, err := MarketplaceBlocks().Blocks[0].Run(context.Background(), &blockClient{}, r)
	require.NoError(t, err)
}

func TestBlockRelationshipArithmetic(t *testing.T) {
	t.Parallel()
	for _, upper := range []bool{false, true} {
		t.Run(map[bool]string{false: "lower", true: "upper"}[upper], func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client := &blockClient{balance: 100000}
			_, err := gamingP2PTrade(ctx, client, &typedDraws{upper: upper})
			require.NoError(t, err)
			vars := client.requests[0].GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction().GetScriptReference().GetVars()
			require.NotEqual(t, vars["from_player"], vars["to_player"], "equal player draws must still select distinct accounts")
			if upper {
				require.Equal(t, "player:20:coins", vars["from_player"])
				require.Equal(t, "player:1:coins", vars["to_player"])
				require.Equal(t, "COINS 100000", vars["amount"])
			} else {
				require.Equal(t, "player:1:coins", vars["from_player"])
				require.Equal(t, "player:2:coins", vars["to_player"])
				require.Equal(t, "COINS 10", vars["amount"])
			}

			client.requests = nil
			_, err = multiCurrencyFX(ctx, client, &typedDraws{upper: upper})
			require.NoError(t, err)
			require.Len(t, client.requests, 2)
			vars = client.requests[0].GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction().GetScriptReference().GetVars()
			target := client.requests[1].GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction()
			require.True(t, target.GetForce())
			require.Len(t, target.GetPostings(), 1)
			posting := target.GetPostings()[0]
			require.Equal(t, "fx:clearing", posting.GetSource())
			if upper {
				require.Equal(t, "treasury:gbp", vars["source_account"])
				require.Equal(t, "GBP/2 50000", vars["amount"])
				require.Equal(t, "treasury:eur", posting.GetDestination())
				require.Equal(t, "EUR/2", posting.GetAsset())
				require.Equal(t, "46000", posting.GetAmount().Dec())
			} else {
				require.Equal(t, "treasury:usd", vars["source_account"])
				require.Equal(t, "USD/2 5000", vars["amount"])
				require.Equal(t, "treasury:eur", posting.GetDestination())
				require.Equal(t, "EUR/2", posting.GetAsset())
				require.Equal(t, "4600", posting.GetAmount().Dec())
			}

			client.requests = nil
			_, err = payrollPaySalary(ctx, client, &typedDraws{upper: upper})
			require.NoError(t, err)
			require.Len(t, client.requests, 1, "funding and salary must remain atomic")
			reqs := client.requests[0].GetUnsigned().GetRequests()
			require.Len(t, reqs, 2)
			fund := reqs[0].GetApply()
			salary := reqs[1].GetApply()
			require.Equal(t, fund.GetLedger(), salary.GetLedger())
			require.Equal(t, "USD/2 500000", fund.GetAction().GetCreateTransaction().GetScriptReference().GetVars()["amount"])
			require.Equal(t, "USD/2 500000", salary.GetAction().GetCreateTransaction().GetScriptReference().GetVars()["amount"])
			if upper {
				require.Equal(t, "dept-ops", salary.GetLedger())
				require.Equal(t, "employee:5", salary.GetAction().GetCreateTransaction().GetScriptReference().GetVars()["employee"])
			} else {
				require.Equal(t, "dept-eng", salary.GetLedger())
				require.Equal(t, "employee:1", salary.GetAction().GetCreateTransaction().GetScriptReference().GetVars()["employee"])
			}
		})
	}
}

func TestRandFuncPreservesOneDraw(t *testing.T) {
	t.Parallel()
	for _, draw := range []struct {
		name string
		draw func(RandFunc) int64
	}{
		{"Intn", func(r RandFunc) int64 { return int64(r.Intn(10)) }},
		{"Int63n", func(r RandFunc) int64 { return r.Int63n(10) }},
	} {
		t.Run(draw.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			r := RandFunc(func() uint64 {
				calls++

				return 0
			})
			require.Equal(t, int64(0), draw.draw(r))
			require.Equal(t, 1, calls, "a fixed legacy source must not enter a rejection loop")
		})
	}
}

func TestRevertSelectsOnlyNonRevertedTransactions(t *testing.T) {
	t.Parallel()
	for _, upper := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "last"}[upper], func(t *testing.T) {
			t.Parallel()
			client := &blockClient{}
			r := &typedDraws{upper: upper}
			_, err := marketplaceRevert(context.Background(), client, r)
			require.NoError(t, err)
			require.Equal(t, []int{2}, r.ints)
			revert := client.requests[0].GetUnsigned().GetRequests()[0].GetApply().GetAction().GetRevertTransaction()
			want := uint64(2)
			if upper {
				want = 3
			}
			require.Equal(t, want, revert.GetTransactionId())
		})
	}
}
