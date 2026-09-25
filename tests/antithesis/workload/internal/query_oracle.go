package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// CreateQueryOracleLedger declares the filtered-read prerequisites in the same
// idempotent proposal as the ledger. In particular, index setup must not add
// success proposals to the failed-bulk audit oracle. Each subsequent oracle
// read waits for the indexes on the replica actually serving that query.
func CreateQueryOracleLedger(ctx context.Context, client servicepb.BucketServiceClient, ledger string, indexes ...commonpb.TransactionBuiltinIndex) error {
	requests := []*servicepb.Request{actions.CreateLedgerAction(ledger, nil)}
	for _, index := range indexes {
		requests = append(requests, actions.CreateBuiltinTxIndexAction(ledger, index))
	}
	key := fmt.Sprintf("create-query-oracle-%016x%016x", Rand().Uint64(), Rand().Uint64())
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(key, requests...))
	// Driver names have a bounded seed space. A collision aborts this scenario
	// without reusing a ledger owned by an earlier invocation.
	if IsAlreadyExists(err) && HasErrorReason(err, domain.ErrReasonLedgerAlreadyExists) {
		return err
	}
	if err != nil {
		reportQueryOracleError(ctx, err, Details{"ledger": ledger, "operation": "create ledger and indexes"})
	}
	return err
}

// ReadOracleTransactions reads one complete page. Ten rows suffice for the
// absence and at-most-one checks; this is not an exhaustive listing API.
// INDEX_BUILDING retries start a fresh query, discarding any partial attempt.
// Readiness is checked by the real linearizable query on its serving replica,
// never inferred from a different node's GetIndexStatus response.
func ReadOracleTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledger string, filter *commonpb.QueryFilter) ([]uint64, error) {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		ids, err := readOracleTransactionPage(readCtx, client, ledger, filter)
		if err == nil {
			return ids, nil
		}
		if HasErrorReason(err, domain.ErrReasonIndexBuilding) && IsUnavailable(err) {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-readCtx.Done():
				timer.Stop()
				return nil, fmt.Errorf("waiting for query oracle index readiness: %w (last error: %v)", readCtx.Err(), err)
			case <-timer.C:
				continue
			}
		}
		reportQueryOracleError(readCtx, err, Details{
			"ledger": ledger, "operation": "list transactions", "filter": filter.String(), "partialTxIds": ids,
		})
		return nil, err
	}
}

func readOracleTransactionPage(ctx context.Context, client servicepb.BucketServiceClient, ledger string, filter *commonpb.QueryFilter) ([]uint64, error) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger: ledger, Options: &commonpb.ListOptions{PageSize: 10, Filter: filter},
	})
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for {
		tx, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return ids, nil
		}
		if err != nil {
			return ids, err
		}
		ids = append(ids, tx.GetId())
	}
}

func reportQueryOracleError(ctx context.Context, err error, details Details) {
	if IsTransient(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		(IsCanceled(err) && ctx.Err() != nil) {
		return
	}
	assert.Unreachable("query oracle encountered a permanent setup or read error", details.With(Details{
		"error": err.Error(), "code": status.Code(err).String(),
	}))
}
