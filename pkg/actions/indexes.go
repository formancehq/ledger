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
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS
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

// CreateAccountAssetIndexAction creates a request for creating the account
// asset-presence builtin index (ACCT_BUILTIN_INDEX_ASSET), which backs the
// `has asset <asset>` account filter.
func CreateAccountAssetIndexAction(ledger string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Id:     &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET}},
			},
		},
	}
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

// indexReadyOnReplica returns nil when the (ledger, IndexID) entry in
// the GetIndexStatus response has a live keyspace and no rewrite in
// flight on the replica the client is talking to: current_version > 0
// and pending_version == 0.
//
// Per-replica version numbers are allocated from the replica's local
// high-water mark (a drop+recreate resumes past the dropped
// incarnation's numbers), so they are NOT comparable to the registry
// row's forward_encoding_version. Callers that need to observe a
// RETYPE completing must capture the pre-retype current_version and
// use indexRewriteDoneOnReplica — the pre-retype keyspace stays live
// (current_version > 0, and pending_version is 0 until the replica's
// builder picks the retype up), so this check alone can pass before
// the rewrite starts.
//
// Each replica advances its IndexVersionState independently as soon as
// its local backfill / rewrite finishes (EN-1323) — readiness is always
// a per-replica question.
func indexReadyOnReplica(resp *servicepb.GetIndexStatusResponse, ledger string, matches func(*commonpb.IndexID) bool, label string) error {
	entry := findIndexEntry(resp, ledger, matches)
	if entry == nil {
		return fmt.Errorf("index %s on %s not found in GetIndexStatus", label, ledger)
	}

	if entry.GetCurrentVersion() == 0 {
		return fmt.Errorf("index %s on %s has current_version=0 (local backfill not switched)", label, ledger)
	}

	if p := entry.GetPendingVersion(); p != 0 {
		return fmt.Errorf("index %s on %s has rewrite to version %d in flight", label, ledger, p)
	}

	return nil
}

// indexRewriteDoneOnReplica returns nil when the replica has switched
// PAST preVersion with no rewrite in flight — the causal completion
// signal for a retype: the pre-retype keyspace stays live throughout
// the rewrite, so only advancement beyond the captured pre-retype
// current_version proves the switch.
func indexRewriteDoneOnReplica(resp *servicepb.GetIndexStatusResponse, ledger string, matches func(*commonpb.IndexID) bool, preVersion uint32, label string) error {
	entry := findIndexEntry(resp, ledger, matches)
	if entry == nil {
		return fmt.Errorf("index %s on %s not found in GetIndexStatus", label, ledger)
	}

	current := entry.GetCurrentVersion()
	if current == preVersion || current == 0 {
		return fmt.Errorf("index %s on %s still serves pre-retype version %d", label, ledger, current)
	}

	if p := entry.GetPendingVersion(); p != 0 {
		return fmt.Errorf("index %s on %s has rewrite to version %d in flight", label, ledger, p)
	}

	return nil
}

func findIndexEntry(resp *servicepb.GetIndexStatusResponse, ledger string, matches func(*commonpb.IndexID) bool) *servicepb.IndexEntry {
	for _, entry := range resp.GetIndexes() {
		if entry.GetLedger() == ledger && matches(entry.GetIndex().GetId()) {
			return entry
		}
	}

	return nil
}

// WaitForMetadataIndexReady polls until the metadata index has been
// atomically switched into a live keyspace on the local replica.
func WaitForMetadataIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, target commonpb.TargetType, key string) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		resp, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexReadyOnReplica(resp, ledger, func(id *commonpb.IndexID) bool {
			m, ok := id.GetKind().(*commonpb.IndexID_Metadata)

			return ok && m.Metadata.GetTarget() == target && m.Metadata.GetKey() == key
		}, fmt.Sprintf("metadata[%s] on %s", key, target.String()))
	})
}

// MetadataIndexCurrentVersion returns the per-replica current_version of the
// (target, key) metadata index on the replica the client is talking to.
// Capture it BEFORE issuing a retype so WaitForMetadataIndexRewrite can
// observe the advancement past it. A replica whose initial backfill has not
// switched yet (current_version == 0) is an error: zero is not a live
// pre-retype keyspace, and waiting for advancement past it would be
// satisfied by the initial switch rather than the requested retype.
//
// The capture and the wait MUST observe the same replica: versions are
// replica-local, so a client whose connection load-balances across nodes
// can capture one replica's version and poll another whose different
// number satisfies the inequality without any rewrite having run. Use a
// per-node connection (as every caller in this repo does).
func MetadataIndexCurrentVersion(ctx context.Context, client servicepb.BucketServiceClient, ledger string, target commonpb.TargetType, key string) (uint32, error) {
	resp, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
	if err != nil {
		return 0, err
	}

	entry := findIndexEntry(resp, ledger, func(id *commonpb.IndexID) bool {
		m, ok := id.GetKind().(*commonpb.IndexID_Metadata)

		return ok && m.Metadata.GetTarget() == target && m.Metadata.GetKey() == key
	})
	if entry == nil {
		return 0, fmt.Errorf("metadata index [%s] on %s not found in GetIndexStatus", key, ledger)
	}

	if entry.GetCurrentVersion() == 0 {
		return 0, fmt.Errorf("metadata index [%s] on %s has no live keyspace yet (current_version=0) — wait for the initial build before capturing a retype token", key, ledger)
	}

	return entry.GetCurrentVersion(), nil
}

// WaitForMetadataIndexRewrite polls until the replica has atomically switched
// the (target, key) metadata index past preVersion (captured before the
// retype) with no rewrite in flight. preVersion must come from
// MetadataIndexCurrentVersion over the SAME per-node connection — see the
// replica-affinity requirement there.
func WaitForMetadataIndexRewrite(ctx context.Context, client servicepb.BucketServiceClient, ledger string, target commonpb.TargetType, key string, preVersion uint32) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		resp, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexRewriteDoneOnReplica(resp, ledger, func(id *commonpb.IndexID) bool {
			m, ok := id.GetKind().(*commonpb.IndexID_Metadata)

			return ok && m.Metadata.GetTarget() == target && m.Metadata.GetKey() == key
		}, preVersion, fmt.Sprintf("metadata[%s] on %s", key, target.String()))
	})
}

// WaitForBuiltinIndexReady polls until a builtin transaction index has been
// atomically switched into a live keyspace on the local replica.
func WaitForBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.TransactionBuiltinIndex) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		resp, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexReadyOnReplica(resp, ledger, func(id *commonpb.IndexID) bool {
			b, ok := id.GetKind().(*commonpb.IndexID_TxBuiltin)

			return ok && b.TxBuiltin == index
		}, "tx_builtin:"+index.String())
	})
}

// WaitForAddressIndexReady polls until an address index has been
// atomically switched into a live keyspace on the local replica.
func WaitForAddressIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, role commonpb.AddressRole) error {
	return WaitForBuiltinIndexReady(ctx, client, ledger, AddressRoleToBuiltinIndex(role))
}

// WaitForAccountAssetIndexReady polls until the account asset-presence builtin
// index has been atomically switched into a live keyspace on the local replica.
func WaitForAccountAssetIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		resp, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexReadyOnReplica(resp, ledger, func(id *commonpb.IndexID) bool {
			b, ok := id.GetKind().(*commonpb.IndexID_AccountBuiltin)

			return ok && b.AccountBuiltin == commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET
		}, "acct_builtin:"+commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET.String())
	})
}

// WaitForLogBuiltinIndexReady polls until a log builtin index has been
// atomically switched into a live keyspace on the local replica.
func WaitForLogBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.LogBuiltinIndex) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		resp, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
		if err != nil {
			return err
		}

		return indexReadyOnReplica(resp, ledger, func(id *commonpb.IndexID) bool {
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
