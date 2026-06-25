package actions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// AddressRoleToBuiltinIndex maps an AddressRole to its corresponding TransactionBuiltinIndex.
func AddressRoleToBuiltinIndex(role commonpb.AddressRole) commonpb.TransactionBuiltinIndex {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS
	default:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS
	}
}

// CreateAddressIndexAction creates a request for creating an address index on a ledger.
func CreateAddressIndexAction(ledger string, role commonpb.AddressRole) *servicepb.Request {
	return CreateBuiltinTxIndexAction(ledger, AddressRoleToBuiltinIndex(role))
}

// DropAddressIndexAction creates a request for dropping an address index.
func DropAddressIndexAction(ledger string, role commonpb.AddressRole) *servicepb.Request {
	return DropBuiltinTxIndexAction(ledger, AddressRoleToBuiltinIndex(role))
}

// CreateLogBuiltinIndexAction creates a request for creating a log builtin index.
func CreateLogBuiltinIndexAction(ledger string, index commonpb.LogBuiltinIndex) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Id:     &commonpb.IndexID{Kind: &commonpb.IndexID_LogBuiltin{LogBuiltin: index}},
			},
		},
	}
}

// indexReadyForLedger streams BucketService.ListIndexes scoped to a single
// ledger and returns nil iff an Index satisfying matches is reported READY.
//
// Replaces the former GetLedger projection path: indexes no longer live on
// LedgerInfo, the registry is consulted explicitly via ListIndexes.
func indexReadyForLedger(ctx context.Context, client servicepb.BucketServiceClient, ledger string, matches func(*commonpb.IndexID) bool, label string) error {
	stream, err := client.ListIndexes(ctx, &servicepb.ListIndexesRequest{
		Scope:  servicepb.ListIndexesRequest_SCOPE_LEDGER,
		Ledger: ledger,
	})
	if err != nil {
		return fmt.Errorf("opening ListIndexes stream: %w", err)
	}

	for {
		idx, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return fmt.Errorf("index %s not found", label)
		}

		if recvErr != nil {
			return fmt.Errorf("streaming ListIndexes: %w", recvErr)
		}

		if !matches(idx.GetId()) {
			continue
		}

		if idx.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
			return nil
		}

		return fmt.Errorf("index %s status is %v, want READY", label, idx.GetBuildStatus())
	}
}

// WaitForMetadataIndexReady polls until a metadata index reaches READY status or the timeout expires.
func WaitForMetadataIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, target commonpb.TargetType, key string) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		return indexReadyForLedger(ctx, client, ledger, func(id *commonpb.IndexID) bool {
			m, ok := id.GetKind().(*commonpb.IndexID_Metadata)

			return ok && m.Metadata.GetTarget() == target && m.Metadata.GetKey() == key
		}, fmt.Sprintf("metadata[%s] on %s", key, target.String()))
	})
}

// WaitForBuiltinIndexReady polls until a builtin transaction index reaches READY status.
func WaitForBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.TransactionBuiltinIndex) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		return indexReadyForLedger(ctx, client, ledger, func(id *commonpb.IndexID) bool {
			b, ok := id.GetKind().(*commonpb.IndexID_TxBuiltin)

			return ok && b.TxBuiltin == index
		}, "tx_builtin:"+index.String())
	})
}

// WaitForAddressIndexReady polls until an address index reaches READY status.
func WaitForAddressIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, role commonpb.AddressRole) error {
	return WaitForBuiltinIndexReady(ctx, client, ledger, AddressRoleToBuiltinIndex(role))
}

// WaitForLogBuiltinIndexReady polls until a log builtin index reaches READY status.
func WaitForLogBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.LogBuiltinIndex) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		return indexReadyForLedger(ctx, client, ledger, func(id *commonpb.IndexID) bool {
			b, ok := id.GetKind().(*commonpb.IndexID_LogBuiltin)

			return ok && b.LogBuiltin == index
		}, "log_builtin:"+index.String())
	})
}

// CountIndexBackfillsInProgress returns the number of indexes currently in
// backfill (Cursor != 0). It replaces the former
// GetIndexStatusResponse.GetBackfillProgress() field count.
func CountIndexBackfillsInProgress(resp *servicepb.GetIndexStatusResponse) int {
	count := 0
	for _, entry := range resp.GetIndexes() {
		if entry.GetCursor() != 0 {
			count++
		}
	}

	return count
}

// poll repeatedly calls check until it returns nil, the timeout expires, or
// ctx is cancelled. The wait between checks is interruptible: cancellation
// returns immediately rather than after the current interval.
func poll(ctx context.Context, timeout, interval time.Duration, check func() error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	timer := time.NewTimer(interval)
	defer timer.Stop()

	var lastErr error
	for {
		if lastErr = check(); lastErr == nil {
			return nil
		}

		timer.Reset(interval)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %w)", ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}
