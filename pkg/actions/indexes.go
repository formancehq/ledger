package actions

import (
	"context"
	"fmt"
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

// indexReadyOnLedger returns nil if an Index entry matching matches is present
// in info.GetIndexes() with READY status.
func indexReadyOnLedger(info *commonpb.LedgerInfo, matches func(*commonpb.IndexID) bool, label string) error {
	for _, idx := range info.GetIndexes() {
		if !matches(idx.GetId()) {
			continue
		}

		if idx.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
			return nil
		}

		return fmt.Errorf("index %s status is %v, want READY", label, idx.GetBuildStatus())
	}

	return fmt.Errorf("index %s not found", label)
}

// WaitForMetadataIndexReady polls until a metadata index reaches READY status or the timeout expires.
func WaitForMetadataIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, target commonpb.TargetType, key string) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexReadyOnLedger(info, func(id *commonpb.IndexID) bool {
			m, ok := id.GetKind().(*commonpb.IndexID_Metadata)

			return ok && m.Metadata.GetTarget() == target && m.Metadata.GetKey() == key
		}, fmt.Sprintf("metadata[%s] on %s", key, target.String()))
	})
}

// WaitForBuiltinIndexReady polls until a builtin transaction index reaches READY status.
func WaitForBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.TransactionBuiltinIndex) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexReadyOnLedger(info, func(id *commonpb.IndexID) bool {
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
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexReadyOnLedger(info, func(id *commonpb.IndexID) bool {
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
