//go:build enable_antithesis_sdk

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

func TestListCompletenessPageBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		pageSize   uint32
		widenFirst bool
		pageCounts []int
	}{
		{"exact_pages", 1, false, []int{1, 1, 1, 0}},
		{"partial_final_page", 2, false, []int{2, 1, 0}},
		{"single_partial_page", 5, false, []int{3, 0}},
		{"oversized_page_sensitivity", 2, true, []int{3, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drivertest.CheckEmissions(t, func() {
				ctx, client := drivertest.StartServer(t)
				const ledger = "listcomp-page-boundaries"
				require.NoError(t, internal.CreateLedger(ctx, client, ledger))
				acked := map[uint64]uint64{}
				var wantIDs []uint64
				for range 3 {
					resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{{
						Source: "world", Destination: "listcomp-dst", Asset: "USD/2", Amount: commonpb.NewUint256FromUint64(1),
					}}, nil)))
					require.NoError(t, err)
					id, ok := actions.GetCreatedTransactionID(resp)
					require.True(t, ok)
					require.NotEmpty(t, resp.GetLogs())
					acked[id] = resp.GetLogs()[len(resp.GetLogs())-1].GetSequence()
					wantIDs = append(wantIDs, id)
				}

				observed := &pageBoundaryClient{BucketServiceClient: client, widenFirst: tc.widenFirst}
				assertListCompleteness(ctx, observed, ledger, acked, 3, tc.pageSize, internal.Details{"ledger": ledger})
				require.Len(t, observed.pages, len(tc.pageCounts), "the empty page must be consumed even after a short page")
				var gotIDs []uint64
				for i, page := range observed.pages {
					require.Len(t, page.ids, tc.pageCounts[i])
					require.Equal(t, tc.pageSize, observed.requests[i].GetOptions().GetPageSize())
					require.True(t, observed.requests[i].GetOptions().GetReverse())
					wantCursor := ""
					if len(gotIDs) > 0 {
						wantCursor = pagecursor.Cursor{Key: fmt.Sprint(gotIDs[len(gotIDs)-1])}.Encode()
					}
					require.Equal(t, wantCursor, observed.requests[i].GetOptions().GetCursor())
					gotIDs = append(gotIDs, page.ids...)
				}
				require.Equal(t, wantIDs, gotIDs, "expectations come from write acknowledgements, not another list")
			}, func(records []drivertest.Assertion) {
				for message, want := range map[string]bool{
					"clean-EOF list pagination contains every acked transaction":           true,
					"clean-EOF list pagination yields no duplicate transaction IDs":        true,
					"clean-EOF list pagination yields strictly increasing transaction IDs": true,
					"clean-EOF list pagination respects the requested page size":           !tc.widenFirst,
				} {
					hits := 0
					for _, record := range records {
						if record.Hit && record.Message == message {
							hits++
							require.Equal(t, want, record.Condition, message)
						}
					}
					require.Equal(t, 1, hits, "assertion must execute: %s", message)
				}
			})
		})
	}
}

// The sensitivity case only widens the first real RPC's page size. All rows
// still come from the engine, once and in order: the original three assertions
// must pass, and only the new page-size assertion must reject the response.
type pageBoundaryClient struct {
	servicepb.BucketServiceClient
	widenFirst bool
	requests   []*servicepb.ListTransactionsRequest
	pages      []*observedPage
}

func (c *pageBoundaryClient) ListTransactions(ctx context.Context, req *servicepb.ListTransactionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Transaction], error) {
	c.requests = append(c.requests, proto.Clone(req).(*servicepb.ListTransactionsRequest))
	forwarded := proto.Clone(req).(*servicepb.ListTransactionsRequest)
	if c.widenFirst && len(c.requests) == 1 {
		forwarded.Options.PageSize = 3
	}
	stream, err := c.BucketServiceClient.ListTransactions(ctx, forwarded, opts...)
	if err != nil {
		return nil, err
	}
	page := &observedPage{ServerStreamingClient: stream}
	c.pages = append(c.pages, page)
	return page, nil
}

type observedPage struct {
	grpc.ServerStreamingClient[commonpb.Transaction]
	ids []uint64
}

func (s *observedPage) Recv() (*commonpb.Transaction, error) {
	tx, err := s.ServerStreamingClient.Recv()
	if err == nil {
		s.ids = append(s.ids, tx.GetId())
	}
	return tx, err
}
