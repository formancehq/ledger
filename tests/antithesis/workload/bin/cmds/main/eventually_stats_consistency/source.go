package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

var errSourceChanged = errors.New("source horizon changed")

func isSourceChanged(err error) bool { return errors.Is(err, errSourceChanged) }

type counts struct {
	Logs     uint64 `json:"logs"`
	Postings uint64 `json:"postings"`
	Reverts  uint64 `json:"reverts"`
}

type expectedLedger struct {
	ID     uint32 `json:"id"`
	Counts counts `json:"counts"`
}

// Each EOF completes one page, never the entire collection when a trailer
// exists. Failed streams discard the fold; cursors must make forward progress.
func readPages[T any](ctx context.Context, open func(context.Context, string) (grpc.ServerStreamingClient[T], error), consume func(*T) error) error {
	seen := map[string]bool{"": true}
	cursor := ""
	for {
		pageCtx, cancel := context.WithCancel(ctx)
		stream, err := open(pageCtx, cursor)
		if err != nil {
			cancel()
			return err
		}
		for {
			item, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				cancel()
				return err
			}
			if err := consume(item); err != nil {
				cancel()
				return err
			}
		}
		values := stream.Trailer().Get("x-next-cursor")
		cancel()
		if len(values) == 0 {
			return nil
		}
		if len(values) != 1 || values[0] == "" || seen[values[0]] {
			return fmt.Errorf("invalid or repeated pagination cursor: %q", values)
		}
		cursor = values[0]
		seen[cursor] = true
	}
}

func barrier(ctx context.Context, client servicepb.BucketServiceClient) (uint64, error) {
	response, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
	if err != nil {
		return 0, err
	}
	if response.GetCommitIndex() == 0 {
		return 0, errors.New("barrier returned zero index")
	}
	return response.GetCommitIndex(), nil
}

func confirmSource(ctx context.Context, client servicepb.BucketServiceClient, expected uint64) error {
	after, err := barrier(ctx, client)
	if err != nil {
		return err
	}
	if after != expected {
		return fmt.Errorf("%w: expected index %d, got %d", errSourceChanged, expected, after)
	}
	return nil
}

// captureSource uses default linearizable reads, then checks that no proposal
// except its closing barrier was committed during the complete paginated fold.
// No usage value is used to derive expectations or qualify source completeness.
func captureSource(ctx context.Context, client servicepb.BucketServiceClient) (map[string]expectedLedger, uint64, error) {
	before, err := barrier(ctx, client)
	if err != nil {
		return nil, 0, err
	}
	ledgers, readErr := collectSource(ctx, client)
	after, err := barrier(ctx, client)
	if err != nil {
		return nil, 0, err
	}
	if after != before+1 {
		return nil, 0, fmt.Errorf("%w during capture: %d -> %d", errSourceChanged, before, after)
	}
	if readErr != nil {
		return nil, 0, readErr
	}
	return ledgers, after, nil
}

func collectSource(ctx context.Context, client servicepb.BucketServiceClient) (map[string]expectedLedger, error) {
	ledgers := make(map[string]expectedLedger)
	err := readPages(ctx, func(ctx context.Context, cursor string) (grpc.ServerStreamingClient[commonpb.LedgerInfo], error) {
		return client.ListLedgers(ctx, &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{PageSize: 100, Cursor: cursor}})
	}, func(info *commonpb.LedgerInfo) error {
		if info.GetName() == "" || info.GetId() == 0 {
			return errors.New("invalid ledger identity")
		}
		if _, exists := ledgers[info.GetName()]; exists {
			return fmt.Errorf("duplicate ledger %q", info.GetName())
		}
		ledgers[info.GetName()] = expectedLedger{ID: info.GetId()}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing ledgers: %w", err)
	}
	for name, ledger := range ledgers {
		ledger.Counts, err = foldLogs(ctx, client, name)
		if err != nil {
			return nil, fmt.Errorf("folding %s: %w", name, err)
		}
		// Boundaries are a separate main-store completeness check: a clean but
		// truncated stream must not become the expected value for usage.
		stats, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: name})
		if err != nil {
			return nil, err
		}
		if stats.GetLogCount() != ledger.Counts.Logs {
			return nil, fmt.Errorf("incomplete source for %s: folded=%+v main logs=%d", name, ledger.Counts, stats.GetLogCount())
		}
		ledgers[name] = ledger
	}
	return ledgers, nil
}

func foldLogs(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (counts, error) {
	var result counts
	var lastID uint64
	err := readPages(ctx, func(ctx context.Context, cursor string) (grpc.ServerStreamingClient[commonpb.Log], error) {
		return client.ListLogs(ctx, &servicepb.ListLogsRequest{Ledger: ledger, Options: &commonpb.ListOptions{PageSize: 100, Cursor: cursor}})
	}, func(entry *commonpb.Log) error {
		apply := entry.GetPayload().GetApply()
		if apply == nil || apply.GetLedgerName() != ledger || apply.GetLog().GetId() != lastID+1 {
			return fmt.Errorf("non-contiguous or foreign log after %d", lastID)
		}
		lastID = apply.GetLog().GetId()
		result.Logs++
		data := apply.GetLog().GetData()
		if data.GetPayload() == nil {
			return errors.New("log has no payload")
		}
		switch payload := data.GetPayload().(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			if payload.CreatedTransaction.GetTransaction() == nil {
				return errors.New("created log has no transaction")
			}
			result.Postings += uint64(len(payload.CreatedTransaction.GetTransaction().GetPostings()))
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			if payload.RevertedTransaction.GetRevertTransaction() == nil {
				return errors.New("revert log has no transaction")
			}
			result.Reverts++
			result.Postings += uint64(len(payload.RevertedTransaction.GetRevertTransaction().GetPostings()))
		}
		return nil
	})
	return result, err
}

func createWitness(ctx context.Context, client servicepb.BucketServiceClient, name string) (*commonpb.LedgerInfo, error) {
	if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(name+"-create", &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: name}}})); err != nil {
		return nil, err
	}
	info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
	if err != nil {
		return nil, err
	}
	if info.GetId() == 0 {
		return nil, errors.New("usage witness has no ledger ID")
	}
	return info, nil
}

// The new reference is an independent usage witness. Its audit entry follows
// every source log. Every replica's sequential usage fold must consume those
// entries before it can expose this reference, regardless of source ledger mode.
func writeWitness(ctx context.Context, client servicepb.BucketServiceClient, ledger string, horizon uint64) (uint64, error) {
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(ledger,
		&servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: ledger, Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
			Reference: ledger, Force: true, Postings: []*commonpb.Posting{{Source: "world", Destination: "witness", Asset: "USD", Amount: commonpb.NewUint256FromUint64(1)}},
		}}}}}},
	))
	if err != nil {
		return 0, err
	}
	after, err := barrier(ctx, client)
	if err != nil {
		return 0, err
	}
	// One atomic witness proposal and one barrier; retries/late writes make the
	// window inconclusive instead of being mistaken for certified quiescence.
	if after != horizon+2 {
		return 0, fmt.Errorf("%w during witness: %d -> %d", errSourceChanged, horizon, after)
	}
	return after, nil
}
