package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// BucketGrpcClient implements Controller by forwarding requests via gRPC to the leader.
type BucketGrpcClient struct {
	client servicepb.BucketServiceClient
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation.
func NewLedgerGrpcClient(client servicepb.BucketServiceClient) *BucketGrpcClient {
	return &BucketGrpcClient{
		client: client,
	}
}

// Barrier forwards a barrier request via gRPC to the leader.
// Returns the Raft commit index at which the barrier was applied.
func (g *BucketGrpcClient) Barrier(ctx context.Context) (uint64, error) {
	resp, err := g.client.Barrier(ctx, &servicepb.BarrierRequest{})
	if err != nil {
		return 0, fmt.Errorf("gRPC Barrier call failed: %w", err)
	}

	return resp.GetCommitIndex(), nil
}

// Apply forwards the envelopes via gRPC to the leader. The authenticated
// caller is captured from the local context and passed alongside, so the
// leader can populate the audit entry with the original subject even
// though the inter-node connection authenticates via cluster-secret.
func (g *BucketGrpcClient) Apply(ctx context.Context, envelopes ...*servicepb.Envelope) ([]*commonpb.Log, error) {
	resp, err := g.client.Apply(ctx, &servicepb.ApplyRequest{
		Envelopes:               envelopes,
		ForwardedCallerSnapshot: auth.ResolveCallerSnapshot(ctx),
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return resp.GetLogs(), nil
}

func (g *BucketGrpcClient) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	resp, err := g.client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: transactionID,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetTransaction(), nil
}

func (g *BucketGrpcClient) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error) {
	var cursorStr string
	if afterTxID > 0 {
		cursorStr = strconv.FormatUint(afterTxID, 10)
	}

	stream, err := g.client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger: ledgerName,
		Options: &commonpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursorStr,
			Reverse:  reverse,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(stream), nil
}

func (g *BucketGrpcClient) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	return g.client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledgerName,
		Address: address,
	})
}

func (g *BucketGrpcClient) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Account], error) {
	stream, err := g.client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger: ledgerName,
		Options: &commonpb.ListOptions{
			PageSize: pageSize,
			Cursor:   afterAddress,
			Reverse:  reverse,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(stream), nil
}

func (g *BucketGrpcClient) ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
	var cursorStr string
	if afterSequence > 0 {
		cursorStr = strconv.FormatUint(afterSequence, 10)
	}

	stream, err := g.client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger: ledgerName,
		Options: &commonpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursorStr,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(stream), nil
}

func (g *BucketGrpcClient) ListLedgers(ctx context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
	// Drain every leader page via x-next-cursor. The Controller.ListLedgers
	// interface does not propagate the caller's cursor to the leader, so a
	// trailer-peek shim would only ever see the first leader page and the
	// follower-side skip predicate would never reach later ledgers. See
	// ListSigningKeys / ListPeriods / ListNumscripts for the same pattern.
	var (
		ledgers []*commonpb.LedgerInfo
		nextCur string
	)

	for {
		stream, err := g.client.ListLedgers(ctx, &servicepb.ListLedgersRequest{
			Options: &commonpb.ListOptions{Cursor: nextCur},
		})
		if err != nil {
			return nil, err
		}

		for {
			ledger, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, recvErr
			}

			ledgers = append(ledgers, ledger)
		}

		if next := nextCursorFromTrailer(stream.Trailer()); next != "" {
			nextCur = next

			continue
		}

		return cursor.NewSliceCursor(ledgers), nil
	}
}

func (g *BucketGrpcClient) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	return g.client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: name,
	})
}

func (g *BucketGrpcClient) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32, ledger string) (cursor.Cursor[*auditpb.AuditEntry], error) {
	var cursorStr string
	if afterSequence != nil {
		cursorStr = strconv.FormatUint(*afterSequence, 10)
	}

	stream, err := g.client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursorStr,
		},
		FailuresOnly: failuresOnly,
		Ledger:       ledger,
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(stream), nil
}

func (g *BucketGrpcClient) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	return g.client.GetLog(ctx, &servicepb.GetLogRequest{
		Sequence: sequence,
	})
}

func (g *BucketGrpcClient) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	return g.client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
		Sequence: sequence,
	})
}

func (g *BucketGrpcClient) ListPeriods(ctx context.Context) (cursor.Cursor[*commonpb.Period], error) {
	// Follow x-next-cursor across pages — see ListSigningKeys for the
	// rationale (routed leader controller caps per-call at the server
	// default page).
	var (
		periods []*commonpb.Period
		nextCur string
	)

	for {
		stream, err := g.client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{
			Options: &commonpb.ListOptions{Cursor: nextCur},
		})
		if err != nil {
			return nil, fmt.Errorf("gRPC ListPeriods call failed: %w", err)
		}

		for {
			period, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, fmt.Errorf("receiving period: %w", recvErr)
			}

			periods = append(periods, period)
		}

		if next := nextCursorFromTrailer(stream.Trailer()); next != "" {
			nextCur = next

			continue
		}

		return cursor.NewSliceCursor(periods), nil
	}
}

func (g *BucketGrpcClient) ListSigningKeys(ctx context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
	// Follow x-next-cursor across pages — when this client wraps a routed
	// (leader) controller the upstream caps each call at the server's
	// default page, so a single stream would only ever return one page.
	var (
		keys    []*commonpb.SigningKey
		nextCur string
	)

	for {
		stream, err := g.client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{
			Options: &commonpb.ListOptions{Cursor: nextCur},
		})
		if err != nil {
			return nil, fmt.Errorf("gRPC ListSigningKeys call failed: %w", err)
		}

		for {
			key, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, fmt.Errorf("receiving signing key: %w", recvErr)
			}

			keys = append(keys, key)
		}

		if next := nextCursorFromTrailer(stream.Trailer()); next != "" {
			nextCur = next

			continue
		}

		return cursor.NewSliceCursor(keys), nil
	}
}

// nextCursorFromTrailer mirrors cmdutil.NextCursorFromTrailer without
// pulling the CLI package into the gRPC adapter.
func nextCursorFromTrailer(trailer metadata.MD) string {
	if vals := trailer.Get(NextCursorTrailerKey); len(vals) > 0 {
		return vals[0]
	}

	return ""
}

func (g *BucketGrpcClient) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	return g.client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
		Ledger: ledgerName,
	})
}

func (g *BucketGrpcClient) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
	stream, err := g.client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
		Ledger:            ledgerName,
		VariableThreshold: variableThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC AnalyzeAccounts stream: %w", err)
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("AnalyzeAccounts stream ended without result")
			}

			return nil, fmt.Errorf("receiving AnalyzeAccounts event: %w", err)
		}

		switch t := event.GetType().(type) {
		case *servicepb.AnalyzeAccountsEvent_Progress:
			if onProgress != nil {
				onProgress(t.Progress.GetProcessed(), t.Progress.GetTotal())
			}
		case *servicepb.AnalyzeAccountsEvent_Result:
			return t.Result, nil
		}
	}
}

func (g *BucketGrpcClient) AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
	stream, err := g.client.AnalyzeTransactions(ctx, &servicepb.AnalyzeTransactionsRequest{
		Ledger:            ledgerName,
		VariableThreshold: variableThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC AnalyzeTransactions stream: %w", err)
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("AnalyzeTransactions stream ended without result")
			}

			return nil, fmt.Errorf("receiving AnalyzeTransactions event: %w", err)
		}

		switch t := event.GetType().(type) {
		case *servicepb.AnalyzeTransactionsEvent_Progress:
			if onProgress != nil {
				onProgress(t.Progress.GetProcessed(), t.Progress.GetTotal())
			}
		case *servicepb.AnalyzeTransactionsEvent_Result:
			return t.Result, nil
		}
	}
}

func (g *BucketGrpcClient) AggregateVolumes(ctx context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
	return g.client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
		Ledger:          ledgerName,
		Filter:          filter,
		UseMaxPrecision: opts.UseMaxPrecision,
		GroupByPrefixes: opts.GroupByPrefixes,
	})
}

func (g *BucketGrpcClient) ListPreparedQueries(ctx context.Context, ledger string) ([]*commonpb.PreparedQuery, error) {
	resp, err := g.client.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetQueries(), nil
}

func (g *BucketGrpcClient) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	return g.client.ExecutePreparedQuery(ctx, req)
}

func (g *BucketGrpcClient) GetLedgerStats(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error) {
	return g.client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{
		Ledger: ledgerName,
	})
}

func (g *BucketGrpcClient) GetNumscript(ctx context.Context, ledger, name string, version string) (*commonpb.NumscriptInfo, error) {
	return g.client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
		Ledger:  ledger,
		Name:    name,
		Version: version,
	})
}

func (g *BucketGrpcClient) ListNumscripts(ctx context.Context, ledger string) ([]*commonpb.NumscriptInfo, error) {
	// Follow x-next-cursor across pages — see ListSigningKeys for the
	// rationale (this client may wrap a routed leader controller whose
	// per-call response is capped at the server default page).
	var (
		scripts []*commonpb.NumscriptInfo
		nextCur string
	)

	for {
		stream, err := g.client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{
			Ledger:  ledger,
			Options: &commonpb.ListOptions{Cursor: nextCur},
		})
		if err != nil {
			return nil, fmt.Errorf("gRPC ListNumscripts call failed: %w", err)
		}

		for {
			info, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, fmt.Errorf("receiving numscript: %w", recvErr)
			}

			scripts = append(scripts, info)
		}

		if next := nextCursorFromTrailer(stream.Trailer()); next != "" {
			nextCur = next

			continue
		}

		return scripts, nil
	}
}

func (g *BucketGrpcClient) GetPeriodSchedule(ctx context.Context) (string, error) {
	resp, err := g.client.GetPeriodSchedule(ctx, &servicepb.GetPeriodScheduleRequest{})
	if err != nil {
		return "", fmt.Errorf("gRPC GetPeriodSchedule call failed: %w", err)
	}

	return resp.GetCron(), nil
}

func (g *BucketGrpcClient) GetEventsSinks(ctx context.Context) ([]*commonpb.SinkConfig, error) {
	resp, err := g.client.GetEventsSinks(ctx, &servicepb.GetEventsSinksRequest{})
	if err != nil {
		return nil, fmt.Errorf("gRPC GetEventsSinks call failed: %w", err)
	}

	return resp.GetSinks(), nil
}

func (g *BucketGrpcClient) InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
	return g.client.InspectIndex(ctx, req)
}

var _ ctrl.Controller = (*BucketGrpcClient)(nil)
