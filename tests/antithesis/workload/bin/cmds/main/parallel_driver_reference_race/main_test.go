package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

func TestDriverQueryOracles(t *testing.T) {
	drivertest.CheckDriver(t, main, "a reference never maps to more than one committed transaction")
}

func TestReferenceOracleDetectsInjectedDuplicate(t *testing.T) {
	const ledger, reference = "refrace-sensitivity", "sensitivity-reference"
	const injection = "second transaction ID injected after a successful real reference query"
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		require.NoError(t, internal.CreateQueryOracleLedger(ctx, client, ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
		resp, err := createWithReference(ctx, client, ledger, reference, "refrace-sensitivity-destination")
		require.NoError(t, err)
		txID, ok := actions.GetCreatedTransactionID(resp)
		require.True(t, ok)

		// Establish the genuine filtered result before injecting a response. The
		// engine has committed exactly one transaction with this reference.
		ids, err := internal.ReadOracleTransactions(ctx, client, ledger, actions.ReferenceFilter(reference))
		require.NoError(t, err)
		require.Equal(t, []uint64{txID}, ids)

		injectedID := ^uint64(0)
		require.NotEqual(t, txID, injectedID)
		interceptor := &duplicateReferenceClient{BucketServiceClient: client, injectedID: injectedID}
		assertReferenceUnique(ctx, interceptor, ledger, reference, internal.Details{
			"ledger": ledger, "sensitivityInjection": injection,
			"expectedTxIds": fmt.Sprint([]uint64{txID, injectedID}),
		})
		require.NotNil(t, interceptor.stream)
		require.True(t, interceptor.stream.injected, "injection must follow a real row and clean EOF")
		require.Equal(t, txID, interceptor.stream.first.GetId())
	}, func(records []drivertest.Assertion) {
		var hits []drivertest.Assertion
		for _, record := range records {
			if record.Hit && record.Message == "a reference never maps to more than one committed transaction" {
				hits = append(hits, record)
			}
		}
		require.Len(t, hits, 1, "the original uniqueness oracle must execute")
		require.False(t, hits[0].Condition)
		require.Equal(t, ledger, hits[0].Details["ledger"])
		require.Equal(t, reference, hits[0].Details["reference"])
		require.Equal(t, injection, hits[0].Details["sensitivityInjection"])
		require.NotEmpty(t, hits[0].Details["expectedTxIds"])
		require.Equal(t, hits[0].Details["expectedTxIds"], hits[0].Details["txIds"])
	})
}

// duplicateReferenceClient delegates every RPC to the real server. Only the
// successful filtered response gains a second distinct ID for oracle sensitivity;
// this does not claim that the engine committed a duplicate reference.
type duplicateReferenceClient struct {
	servicepb.BucketServiceClient
	injectedID uint64
	stream     *duplicateReferenceStream
}

func (c *duplicateReferenceClient) ListTransactions(ctx context.Context, req *servicepb.ListTransactionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Transaction], error) {
	stream, err := c.BucketServiceClient.ListTransactions(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	c.stream = &duplicateReferenceStream{ServerStreamingClient: stream, injectedID: c.injectedID}
	return c.stream, nil
}

type duplicateReferenceStream struct {
	grpc.ServerStreamingClient[commonpb.Transaction]
	first      *commonpb.Transaction
	injectedID uint64
	injected   bool
	finished   bool
}

func (s *duplicateReferenceStream) Recv() (*commonpb.Transaction, error) {
	if s.finished {
		return nil, io.EOF
	}
	tx, err := s.ServerStreamingClient.Recv()
	if err == nil {
		if s.first == nil {
			s.first = tx
		}
		return tx, nil
	}
	if errors.Is(err, io.EOF) {
		s.finished = true
		if s.first != nil {
			duplicate := proto.Clone(s.first).(*commonpb.Transaction)
			duplicate.Id = s.injectedID
			s.injected = true
			return duplicate, nil
		}
	}
	return nil, err
}
