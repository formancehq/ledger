package grpc

import (
	"context"
	"errors"
	"io"
	"strconv"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/query"
)

// BucketGrpcClient implements Controller by forwarding requests via gRPC to the leader.
type BucketGrpcClient struct {
	client                ledgerpb.BucketServiceClient
	trustedPeerForwarding bool
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation.
func NewLedgerGrpcClient(client ledgerpb.BucketServiceClient, trustedPeerForwarding ...bool) *BucketGrpcClient {
	trusted := len(trustedPeerForwarding) > 0 && trustedPeerForwarding[0]

	return &BucketGrpcClient{
		client:                client,
		trustedPeerForwarding: trusted,
	}
}

// Barrier forwards a barrier request via gRPC to the leader.
// Returns the Raft commit index at which the barrier was applied.
func (g *BucketGrpcClient) Barrier(ctx context.Context) (uint64, error) {
	resp, err := g.client.Barrier(ctx, &ledgerpb.BarrierRequest{})
	if err != nil {
		return 0, err
	}

	return resp.GetCommitIndex(), nil
}

// Apply forwards the batch via gRPC to the leader. When authentication is
// enabled, the caller is captured from the local context and stamped onto the
// wrapper, so the leader can populate the audit entry with the original
// subject even though the inter-node connection authenticates via
// cluster-secret. Auth-disabled attribution is derived locally by every node.
// The signed/unsigned variant rides through unchanged for leader-side
// verification.
func (g *BucketGrpcClient) Apply(ctx context.Context, req *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
	caller := auth.ResolveCallerSnapshot(ctx)
	// Auth-disabled attribution is derived from the leader's immutable local
	// auth state as well. Do not put it on the wire: clusters may intentionally
	// run without a cluster secret when TLS is disabled, and in that topology
	// the service endpoint cannot distinguish a peer from a public client.
	// Authenticated and anonymous callers still require the cluster-secret trust
	// boundary because their original identity/effective scopes must be frozen.
	if caller.GetAuthDisabled() == nil || g.trustedPeerForwarding {
		req.ForwardedCallerSnapshot = caller
	} else {
		req.ForwardedCallerSnapshot = nil
	}

	var trailers metadata.MD
	resp, err := g.client.Apply(ctx, req, ggrpc.Trailer(&trailers))
	if err != nil {
		return nil, err
	}

	// This is response metadata from the leader, never an incoming client hint.
	values := trailers.Get(metadataKeyApplyReplayed)
	if len(values) != 1 || (values[0] != "true" && values[0] != "false") {
		return nil, errors.New("leader Apply response missing valid execution provenance")
	}

	return &domain.ApplyResult{Logs: resp.GetLogs(), Replayed: values[0] == "true"}, nil
}

func (g *BucketGrpcClient) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*ledgerpb.Transaction, error) {
	resp, err := g.client.GetTransaction(ctx, &ledgerpb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: transactionID,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetTransaction(), nil
}

func (g *BucketGrpcClient) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *ledgerpb.QueryFilter, reverse bool) (cursor.Cursor[*ledgerpb.Transaction], error) {
	var cursorStr string
	if afterTxID > 0 {
		cursorStr = strconv.FormatUint(afterTxID, 10)
	}

	stream, err := g.client.ListTransactions(ctx, &ledgerpb.ListTransactionsRequest{
		Ledger: ledgerName,
		Options: &ledgerpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursorStr,
			Reverse:  reverse,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(ctx, stream), nil
}

func (g *BucketGrpcClient) GetAccount(ctx context.Context, ledgerName string, address string, opts ctrl.GetAccountOptions) (*ledgerpb.Account, error) {
	return g.client.GetAccount(ctx, &ledgerpb.GetAccountRequest{
		Ledger:         ledgerName,
		Address:        address,
		CollapseColors: opts.CollapseColors,
	})
}

func (g *BucketGrpcClient) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *ledgerpb.QueryFilter, reverse bool) (cursor.Cursor[*ledgerpb.Account], error) {
	stream, err := g.client.ListAccounts(ctx, &ledgerpb.ListAccountsRequest{
		Ledger: ledgerName,
		Options: &ledgerpb.ListOptions{
			PageSize: pageSize,
			Cursor:   afterAddress,
			Reverse:  reverse,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(ctx, stream), nil
}

func (g *BucketGrpcClient) ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *ledgerpb.QueryFilter) (cursor.Cursor[*ledgerpb.Log], error) {
	var cursorStr string
	if afterSequence > 0 {
		cursorStr = strconv.FormatUint(afterSequence, 10)
	}

	stream, err := g.client.ListLogs(ctx, &ledgerpb.ListLogsRequest{
		Ledger: ledgerName,
		Options: &ledgerpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursorStr,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(ctx, stream), nil
}

func (g *BucketGrpcClient) ListLedgers(ctx context.Context) (cursor.Cursor[*ledgerpb.LedgerInfo], error) {
	// Drain every leader page via x-next-cursor. The Controller.ListLedgers
	// interface does not propagate the caller's cursor to the leader, so a
	// trailer-peek shim would only ever see the first leader page and the
	// follower-side skip predicate would never reach later ledgers. See
	// ListSigningKeys / ListNumscripts for the same pattern.
	var (
		ledgers []*ledgerpb.LedgerInfo
		nextCur string
	)

	for {
		stream, err := g.client.ListLedgers(ctx, &ledgerpb.ListLedgersRequest{
			Options: &ledgerpb.ListOptions{Cursor: nextCur},
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

func (g *BucketGrpcClient) GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	return g.client.GetLedger(ctx, &ledgerpb.GetLedgerRequest{
		Ledger: name,
	})
}

func (g *BucketGrpcClient) ListAuditEntries(ctx context.Context, pageSize uint32, afterSequence uint64, filter *ledgerpb.QueryFilter, reverse bool) (cursor.Cursor[*ledgerpb.AuditEntry], error) {
	var cursorStr string
	if afterSequence > 0 {
		cursorStr = strconv.FormatUint(afterSequence, 10)
	}

	stream, err := g.client.ListAuditEntries(ctx, &ledgerpb.ListAuditEntriesRequest{
		Options: &ledgerpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursorStr,
			Reverse:  reverse,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	return NewUpstreamPeekCursor(ctx, stream), nil
}

func (g *BucketGrpcClient) GetLog(ctx context.Context, sequence uint64) (*ledgerpb.Log, error) {
	return g.client.GetLog(ctx, &ledgerpb.GetLogRequest{
		Sequence: sequence,
	})
}

func (g *BucketGrpcClient) GetAuditEntry(ctx context.Context, sequence uint64) (*ledgerpb.AuditEntry, error) {
	return g.client.GetAuditEntry(ctx, &ledgerpb.GetAuditEntryRequest{
		Sequence: sequence,
	})
}

func (g *BucketGrpcClient) ListSigningKeys(ctx context.Context) (cursor.Cursor[*ledgerpb.SigningKey], error) {
	// Follow x-next-cursor across pages — when this client wraps a routed
	// (leader) controller the upstream caps each call at the server's
	// default page, so a single stream would only ever return one page.
	var (
		keys    []*ledgerpb.SigningKey
		nextCur string
	)

	for {
		stream, err := g.client.ListSigningKeys(ctx, &ledgerpb.ListSigningKeysRequest{
			Options: &ledgerpb.ListOptions{Cursor: nextCur},
		})
		if err != nil {
			return nil, err
		}

		for {
			key, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, recvErr
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

func (g *BucketGrpcClient) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*ledgerpb.GetMetadataSchemaStatusResponse, error) {
	return g.client.GetMetadataSchemaStatus(ctx, &ledgerpb.GetMetadataSchemaStatusRequest{
		Ledger: ledgerName,
	})
}

func (g *BucketGrpcClient) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*ledgerpb.AnalyzeAccountsResponse, error) {
	stream, err := g.client.AnalyzeAccounts(ctx, &ledgerpb.AnalyzeAccountsRequest{
		Ledger:            ledgerName,
		VariableThreshold: variableThreshold,
	})
	if err != nil {
		return nil, err
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("AnalyzeAccounts stream ended without result")
			}

			return nil, err
		}

		switch t := event.GetType().(type) {
		case *ledgerpb.AnalyzeAccountsEvent_Progress:
			if onProgress != nil {
				onProgress(t.Progress.GetProcessed(), t.Progress.GetTotal())
			}
		case *ledgerpb.AnalyzeAccountsEvent_Result:
			return t.Result, nil
		}
	}
}

func (g *BucketGrpcClient) AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*ledgerpb.AnalyzeTransactionsResponse, error) {
	stream, err := g.client.AnalyzeTransactions(ctx, &ledgerpb.AnalyzeTransactionsRequest{
		Ledger:            ledgerName,
		VariableThreshold: variableThreshold,
	})
	if err != nil {
		return nil, err
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("AnalyzeTransactions stream ended without result")
			}

			return nil, err
		}

		switch t := event.GetType().(type) {
		case *ledgerpb.AnalyzeTransactionsEvent_Progress:
			if onProgress != nil {
				onProgress(t.Progress.GetProcessed(), t.Progress.GetTotal())
			}
		case *ledgerpb.AnalyzeTransactionsEvent_Result:
			return t.Result, nil
		}
	}
}

func (g *BucketGrpcClient) AggregateVolumes(ctx context.Context, ledgerName string, filter *ledgerpb.QueryFilter, opts query.AggregateOptions) (*ledgerpb.AggregateResult, error) {
	return g.client.AggregateVolumes(ctx, &ledgerpb.AggregateVolumesRequest{
		Ledger:          ledgerName,
		Filter:          filter,
		UseMaxPrecision: opts.UseMaxPrecision,
		CollapseColors:  opts.CollapseColors,
		GroupByPrefixes: opts.GroupByPrefixes,
	})
}

func (g *BucketGrpcClient) ListPreparedQueries(ctx context.Context, ledger string) ([]*ledgerpb.PreparedQuery, error) {
	resp, err := g.client.ListPreparedQueries(ctx, &ledgerpb.ListPreparedQueriesRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetQueries(), nil
}

func (g *BucketGrpcClient) ExecutePreparedQuery(ctx context.Context, req *ledgerpb.ExecutePreparedQueryRequest) (*ledgerpb.ExecutePreparedQueryResponse, error) {
	return g.client.ExecutePreparedQuery(ctx, req)
}

func (g *BucketGrpcClient) GetLedgerStats(ctx context.Context, ledgerName string) (*ledgerpb.LedgerStats, error) {
	return g.client.GetLedgerStats(ctx, &ledgerpb.GetLedgerStatsRequest{
		Ledger: ledgerName,
	})
}

func (g *BucketGrpcClient) GetNumscript(ctx context.Context, ledger, name string, version string) (*ledgerpb.NumscriptInfo, error) {
	return g.client.GetNumscript(ctx, &ledgerpb.GetNumscriptRequest{
		Ledger:  ledger,
		Name:    name,
		Version: version,
	})
}

func (g *BucketGrpcClient) GetTemplateUsage(ctx context.Context, ledger, name string) (*ledgerpb.TemplateUsage, error) {
	return g.client.GetTemplateUsage(ctx, &ledgerpb.GetTemplateUsageRequest{
		Ledger: ledger,
		Name:   name,
	})
}

func (g *BucketGrpcClient) ListNumscripts(ctx context.Context, ledger string) ([]*ledgerpb.NumscriptInfo, error) {
	// Follow x-next-cursor across pages — see ListSigningKeys for the
	// rationale (this client may wrap a routed leader controller whose
	// per-call response is capped at the server default page).
	var (
		scripts []*ledgerpb.NumscriptInfo
		nextCur string
	)

	for {
		stream, err := g.client.ListNumscripts(ctx, &ledgerpb.ListNumscriptsRequest{
			Ledger:  ledger,
			Options: &ledgerpb.ListOptions{Cursor: nextCur},
		})
		if err != nil {
			return nil, err
		}

		for {
			info, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, recvErr
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

func (g *BucketGrpcClient) ListNumscriptVersions(ctx context.Context, ledger, name string) (string, []*ledgerpb.NumscriptVersionEntry, error) {
	resp, err := g.client.ListNumscriptVersions(ctx, &ledgerpb.ListNumscriptVersionsRequest{
		Ledger: ledger,
		Name:   name,
	})
	if err != nil {
		return "", nil, err
	}

	return resp.GetLatestVersion(), resp.GetVersions(), nil
}

func (g *BucketGrpcClient) GetEventsSinks(ctx context.Context) ([]*ledgerpb.SinkConfig, []*ledgerpb.SinkStatus, error) {
	resp, err := g.client.GetEventsSinks(ctx, &ledgerpb.GetEventsSinksRequest{})
	if err != nil {
		return nil, nil, err
	}

	return resp.GetSinks(), resp.GetSinkStatuses(), nil
}

func (g *BucketGrpcClient) InspectIndex(ctx context.Context, req *ledgerpb.InspectIndexRequest) (*ledgerpb.InspectIndexResponse, error) {
	return g.client.InspectIndex(ctx, req)
}

func (g *BucketGrpcClient) GetIndexStatus(ctx context.Context, req *ledgerpb.GetIndexStatusRequest) (*ledgerpb.GetIndexStatusResponse, error) {
	return g.client.GetIndexStatus(ctx, req)
}

func (g *BucketGrpcClient) GetIndex(ctx context.Context, req *ledgerpb.GetIndexRequest) (*ledgerpb.Index, error) {
	return g.client.GetIndex(ctx, req)
}

func (g *BucketGrpcClient) GetIndexEntryStatus(ctx context.Context, req *ledgerpb.GetIndexEntryStatusRequest) (*ledgerpb.IndexEntry, error) {
	return g.client.GetIndexEntryStatus(ctx, req)
}

func (g *BucketGrpcClient) ListIndexes(ctx context.Context, req *ledgerpb.ListIndexesRequest) (cursor.Cursor[*ledgerpb.Index], error) {
	stream, err := g.client.ListIndexes(ctx, req)
	if err != nil {
		return nil, err
	}

	// Stream lazily rather than draining the whole registry into a slice on
	// the follower: NewUpstreamPeekCursor yields each Index as Recv delivers
	// it, so the first item is available before the leader sends EOF and peak
	// memory is O(1) instead of O(total). Matches ListTransactions /
	// ListAccounts / ListLogs.
	return NewUpstreamPeekCursor(ctx, stream), nil
}

var _ ctrl.Controller = (*BucketGrpcClient)(nil)
