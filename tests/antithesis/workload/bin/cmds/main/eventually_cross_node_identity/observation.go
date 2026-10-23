package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

type ledgerState struct {
	ID       uint32 `json:"id"`
	Accounts uint64 `json:"accounts"`
	Digest   string `json:"digest"`
}

type auditState struct {
	Count       uint64 `json:"count"`
	Last        uint64 `json:"last"`
	Version     uint32 `json:"version"`
	Digest      string `json:"digest"`
	TipDigest   string `json:"tipDigest"`
	TipPresent  bool   `json:"tipPresent"`
	TailMatches bool   `json:"tailMatches"`
}

type replicaState struct {
	Ledgers map[string]ledgerState `json:"ledgers"`
	Audit   auditState             `json:"audit"`
}

func equalAccounts(a, b replicaState) bool { return maps.Equal(a.Ledgers, b.Ledgers) }

// Fold complete local collections into deterministic, length-delimited digests.
// Retain ledger summaries and pagination cursors, not every account/audit row.
// Include full colored volumes, metadata and timestamps; account and
// ledger omissions change the count/digest rather than being silently skipped.
func readReplica(ctx context.Context, client servicepb.BucketServiceClient) (replicaState, error) {
	result := replicaState{Ledgers: make(map[string]ledgerState)}
	var ledgers []*commonpb.LedgerInfo
	err := readPages(ctx, func(ctx context.Context, cursor string) (grpc.ServerStreamingClient[commonpb.LedgerInfo], error) {
		return client.ListLedgers(ctx, &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{PageSize: 100, Cursor: cursor}})
	}, func(info *commonpb.LedgerInfo) error {
		if info.GetName() == "" || info.GetId() == 0 {
			return errors.New("invalid ledger identity")
		}
		if _, exists := result.Ledgers[info.GetName()]; exists {
			return fmt.Errorf("duplicate ledger %q", info.GetName())
		}
		result.Ledgers[info.GetName()] = ledgerState{ID: info.GetId()}
		ledgers = append(ledgers, info)

		return nil
	})
	if err != nil {
		return result, fmt.Errorf("list ledgers: %w", err)
	}
	for _, ledger := range ledgers {
		digest := sha256.New()
		// Mirror progress is also derived from committed rows (boundary, source
		// head, recorded status), so it must agree at this exact durable horizon.
		if err := digestMessage(digest, ledger); err != nil {
			return result, err
		}
		state := ledgerState{ID: ledger.GetId()}
		var lastAddress string
		err := readPages(ctx, func(ctx context.Context, cursor string) (grpc.ServerStreamingClient[commonpb.Account], error) {
			return client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger.GetName(), Options: &commonpb.ListOptions{PageSize: 100, Cursor: cursor}})
		}, func(account *commonpb.Account) error {
			if account.GetAddress() == "" || (state.Accounts > 0 && account.GetAddress() <= lastAddress) {
				return errors.New("unordered or duplicate account in complete listing")
			}
			lastAddress = account.GetAddress()
			state.Accounts++

			return digestMessage(digest, account)
		})
		if err != nil {
			return result, fmt.Errorf("list accounts for %s: %w", ledger.GetName(), err)
		}
		state.Digest = hex.EncodeToString(digest.Sum(nil))
		result.Ledgers[ledger.GetName()] = state
	}
	result.Audit, err = readAudit(ctx, client)

	return result, err
}

func readAudit(ctx context.Context, client servicepb.BucketServiceClient) (auditState, error) {
	var result auditState
	digest := sha256.New()
	var last *auditpb.AuditEntry
	err := readPages(ctx, func(ctx context.Context, cursor string) (grpc.ServerStreamingClient[auditpb.AuditEntry], error) {
		return client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{PageSize: 100, Cursor: cursor}})
	}, func(entry *auditpb.AuditEntry) error {
		if entry.GetSequence() == 0 || entry.GetSequence() <= result.Last {
			return errors.New("unordered or duplicate audit sequence")
		}
		result.Count++
		result.Last, result.Version, last = entry.GetSequence(), entry.GetHashVersion(), entry

		return digestMessage(digest, entry)
	})
	if err != nil {
		return result, fmt.Errorf("complete audit stream: %w", err)
	}
	result.Digest = hex.EncodeToString(digest.Sum(nil))

	// Independently read the actual tail: a clean EOF after a silently truncated
	// first page must not qualify as the complete audit chain. Read through EOF
	// even for this one-item page so a partial stream error is never accepted.
	stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{PageSize: 1, Reverse: true}})
	if err != nil {
		return result, err
	}
	var tail *auditpb.AuditEntry
	for {
		entry, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result, err
		}
		if tail != nil {
			return result, errors.New("audit tail page exceeded requested size")
		}
		tail = entry
	}
	result.TailMatches = proto.Equal(last, tail)
	if tail == nil {
		return result, nil
	}
	entry, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{Sequence: tail.GetSequence()})
	if err != nil {
		if internal.IsNotFound(err) {
			// At a qualified exact durable horizon, absence is a real observation,
			// not transient skew. Keep it for the deferred integrity verdict.
			return result, nil
		}

		return result, err
	}
	if entry == nil {
		return result, errors.New("audit tip response is nil")
	}
	result.TipPresent = true
	tipDigest := sha256.New()
	if err := digestMessage(tipDigest, entry); err != nil {
		return result, err
	}
	result.TipDigest = hex.EncodeToString(tipDigest.Sum(nil))
	// List strips items; all other header fields must match the point read.
	entry.Items = nil
	result.TailMatches = result.TailMatches && proto.Equal(entry, tail)

	return result, nil
}

func digestMessage(digest hash.Hash, message proto.Message) error {
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return err
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(data)))
	if _, err := digest.Write(length[:]); err != nil {
		return err
	}
	_, err = digest.Write(data)

	return err
}

// EOF completes a page. Only absence of a continuation trailer completes the
// collection; transport errors and repeated cursors invalidate the whole sweep.
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
			return fmt.Errorf("invalid or repeated cursor %q", values)
		}
		cursor = values[0]
		seen[cursor] = true
	}
}
