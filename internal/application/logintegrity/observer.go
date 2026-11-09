package logintegrity

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	observeInterval = 250 * time.Millisecond
	peerTimeout     = 2 * time.Second
	initialWindow   = 256
)

type clusterView interface {
	IsLeader() bool
	GetNodeID() uint64
	MemberIDs() []uint64
}

type peerConnections interface {
	GetPeerConnection(uint64) *grpc.ClientConn
}

type peerCursor struct {
	next         uint64
	done         bool
	lastMismatch string
	deferredHead bool
}

// Observer compares the immutable log stream against each cluster member.
// It runs on the leader and only reports discrepancies; it never gates writes
// or chooses which replica is correct.
type Observer struct {
	store   *dal.Store
	cluster clusterView
	peers   peerConnections
	local   rafttransportpb.LogIntegrityServiceServer
	logger  logging.Logger

	cursors         map[uint64]*peerCursor
	active          bool
	lastSampleError string

	divergences   metric.Int64Counter
	unavailable   metric.Int64Counter
	localDataLoss metric.Int64Counter
	checkedLogs   metric.Int64Counter
	duration      metric.Float64Histogram
}

func NewObserver(store *dal.Store, cluster clusterView, peers peerConnections, local rafttransportpb.LogIntegrityServiceServer, logger logging.Logger, meter metric.Meter) *Observer {
	o := &Observer{
		store: store, cluster: cluster, peers: peers, local: local,
		logger:  logger.WithField("component", "log-integrity"),
		cursors: make(map[uint64]*peerCursor),
	}
	o.divergences, _ = meter.Int64Counter("log_integrity.divergences", metric.WithUnit("{range}"))
	o.unavailable, _ = meter.Int64Counter("log_integrity.unavailable_attempts", metric.WithUnit("{attempt}"))
	o.localDataLoss, _ = meter.Int64Counter("log_integrity.local_data_loss", metric.WithUnit("{range}"))
	o.checkedLogs, _ = meter.Int64Counter("log_integrity.checked_logs", metric.WithUnit("{log}"))
	o.duration, _ = meter.Float64Histogram("log_integrity.comparison.duration", metric.WithUnit("s"))

	return o
}

// Run polls until cancellation. It starts after the Raft listener and node;
// cancellation is bounded by the per-peer RPC timeout.
func (o *Observer) Run(ctx context.Context) {
	ticker := time.NewTicker(observeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.observe(ctx)
		}
	}
}

func (o *Observer) observe(ctx context.Context) {
	if !o.cluster.IsLeader() {
		o.active = false
		clear(o.cursors)
		o.lastSampleError = ""

		return
	}
	if !o.active {
		o.active = true
		clear(o.cursors)
	}

	// The applied marker and last log key come from the same snapshot. Every
	// peer must reach this marker before its digest can be compared.
	handle, err := o.store.NewReadHandle()
	if err != nil {
		o.logger.Errorf("Cannot open local log snapshot: %v", err)

		return
	}
	applied, appliedErr := query.ReadLastAppliedIndex(handle)
	head, headErr := LastLogSequence(handle)
	_ = handle.Close() // The read result is already complete; Close releases the snapshot and lock.
	if appliedErr != nil || headErr != nil {
		problem := fmt.Sprintf("applied=%v head=%v", appliedErr, headErr)
		if problem != o.lastSampleError {
			o.lastSampleError = problem
			o.localDataLoss.Add(ctx, 1)
			o.logger.Errorf("Cannot sample local log head: %s", problem)
		}

		return
	}
	o.lastSampleError = ""
	members := o.cluster.MemberIDs()
	slices.Sort(members)
	var checks sync.WaitGroup
	for _, id := range members {
		if id == o.cluster.GetNodeID() {
			continue
		}
		if !o.cluster.IsLeader() {
			break
		}
		cursor := o.cursors[id]
		if cursor == nil {
			first := uint64(1)
			if head > initialWindow {
				first = head - initialWindow + 1
			}
			cursor = &peerCursor{next: first}
			o.cursors[id] = cursor
		}
		// Probe once even with an empty local log so a peer suffix is visible.
		if cursor.done || (head != 0 && cursor.next > head && !cursor.deferredHead) {
			continue
		}
		first := cursor.next
		if head != 0 && first > head {
			first = head
		}
		last := head
		if head == 0 {
			last = 1
		}
		if head != 0 && head-first >= MaxRangeEntries {
			last = first + MaxRangeEntries - 1
		}
		checks.Go(func() {
			o.comparePeerAtHead(ctx, id, cursor, first, last, applied, head)
		})
	}
	checks.Wait()
	// A removed peer must not retain a stale cursor if its ID is later reused.
	for id := range o.cursors {
		if !slices.Contains(members, id) {
			delete(o.cursors, id)
		}
	}
}

func (o *Observer) comparePeer(ctx context.Context, id uint64, cursor *peerCursor, first, last, applied uint64) {
	o.comparePeerAtHead(ctx, id, cursor, first, last, applied, last)
}

func (o *Observer) comparePeerAtHead(ctx context.Context, id uint64, cursor *peerCursor, first, last, applied, localHead uint64) {
	started := time.Now()
	attrs := metric.WithAttributes(attribute.String("peer_id", strconv.FormatUint(id, 10)))
	req := &rafttransportpb.GetRangeDigestRequest{
		FirstLogSequence: first, LastLogSequence: last, MinAppliedIndex: applied,
	}
	local, err := o.local.GetRangeDigest(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		switch {
		case status.Code(err) == codes.NotFound && localHead == 0:
			// There is no local digest for an empty log; still query the peer
			// so a non-empty peer suffix can be reported as a divergence.
		case status.Code(err) == codes.DataLoss:
			key := fmt.Sprintf("%d:%d:local", first, last)
			if cursor.lastMismatch != key {
				cursor.lastMismatch = key
				o.localDataLoss.Add(ctx, 1)
				o.logger.Errorf("Cannot digest local logs [%d,%d]: %v", first, last, err)
			}
			advanceCursor(cursor, last)
		default:
			o.logger.Errorf("Cannot digest local logs [%d,%d]: %v", first, last, err)

			return
		}
	}
	if err == nil && (local == nil || len(local.GetDigest()) != DigestSize) {
		o.logger.Errorf("Invalid local digest response for logs [%d,%d]", first, last)

		return
	}
	conn := o.peers.GetPeerConnection(id)
	if conn == nil {
		o.unavailable.Add(ctx, 1, metric.WithAttributes(attribute.String("peer_id", strconv.FormatUint(id, 10)), attribute.String("reason", "no_connection")))

		return
	}
	callCtx, cancel := context.WithTimeout(ctx, peerTimeout)
	defer cancel()
	remote, err := rafttransportpb.NewLogIntegrityServiceClient(conn).GetRangeDigest(callCtx, req)
	if err != nil {
		if localHead == 0 && status.Code(err) == codes.NotFound {
			return
		}
		if status.Code(err) == codes.DataLoss || status.Code(err) == codes.NotFound {
			o.reportMismatch(ctx, id, cursor, first, last, applied, fmt.Sprintf("peer data loss: %v", err))
			advanceCursor(cursor, last)
		} else {
			o.unavailable.Add(ctx, 1, metric.WithAttributes(attribute.String("peer_id", strconv.FormatUint(id, 10)), attribute.String("reason", status.Code(err).String())))
		}

		return
	}
	localApplied := applied
	comparisonHead := localHead
	if local != nil {
		localApplied = local.GetAppliedIndex()
		comparisonHead = local.GetLastLogSequenceOnNode()
	}
	if remote.GetLastLogSequenceOnNode() > comparisonHead && remote.GetAppliedIndex() == localApplied {
		cursor.deferredHead = false
		o.reportMismatch(ctx, id, cursor, first, last, applied, fmt.Sprintf("peer log head %d exceeds local head %d", remote.GetLastLogSequenceOnNode(), comparisonHead))
		advanceCursor(cursor, last)

		return
	}
	cursor.deferredHead = remote.GetLastLogSequenceOnNode() > comparisonHead
	if localHead == 0 {
		return
	}
	if remote.GetDigestVersion() != local.GetDigestVersion() ||
		remote.GetFirstLogSequence() != first || remote.GetLastLogSequence() != last ||
		remote.GetAppliedIndex() < applied || remote.GetLastLogSequenceOnNode() < last ||
		len(remote.GetDigest()) != DigestSize {
		o.unavailable.Add(ctx, 1, metric.WithAttributes(attribute.String("peer_id", strconv.FormatUint(id, 10)), attribute.String("reason", "incompatible_response")))

		return
	}
	if reason := mismatchReason(local, remote); reason != "" {
		o.reportMismatch(ctx, id, cursor, first, last, applied, reason)
		if sequence, err := o.firstDifference(callCtx, conn, first, last, applied); err == nil {
			o.logger.WithFields(map[string]any{
				"peer_id": id, "first_divergent_log_sequence": sequence,
				"first_log_sequence": first, "last_log_sequence": last,
			}).Errorf("First differing persisted log located")
		}
		advanceCursor(cursor, last)

		return
	}
	advanceCursor(cursor, last)
	cursor.lastMismatch = ""
	o.checkedLogs.Add(ctx, int64(last-first+1), attrs)
	o.duration.Record(ctx, time.Since(started).Seconds(), attrs)
}

func advanceCursor(cursor *peerCursor, last uint64) {
	if last == ^uint64(0) {
		cursor.done = true
	} else {
		cursor.next = last + 1
	}
}

func mismatchReason(local, remote *rafttransportpb.GetRangeDigestResponse) string {
	if !bytes.Equal(remote.GetDigest(), local.GetDigest()) {
		return fmt.Sprintf("digest differs: local=%s peer=%s", hex.EncodeToString(local.GetDigest()), hex.EncodeToString(remote.GetDigest()))
	}

	return ""
}

// firstDifference bisects a mismatched range using the remaining peer RPC
// deadline. The original alarm has already been emitted, so failure to locate
// a sequence does not hide the divergence.
func (o *Observer) firstDifference(ctx context.Context, conn *grpc.ClientConn, first, last, applied uint64) (uint64, error) {
	for first < last {
		mid := first + (last-first)/2
		req := &rafttransportpb.GetRangeDigestRequest{
			FirstLogSequence: first, LastLogSequence: mid, MinAppliedIndex: applied,
		}
		local, err := o.local.GetRangeDigest(ctx, req)
		if err != nil {
			return 0, err
		}
		remote, err := rafttransportpb.NewLogIntegrityServiceClient(conn).GetRangeDigest(ctx, req)
		if err != nil {
			return 0, err
		}
		if local == nil || remote == nil || len(local.GetDigest()) != DigestSize || len(remote.GetDigest()) != DigestSize ||
			local.GetDigestVersion() != remote.GetDigestVersion() || remote.GetAppliedIndex() < applied ||
			remote.GetFirstLogSequence() != first || remote.GetLastLogSequence() != mid {
			return 0, errors.New("incompatible digest response while locating first difference")
		}
		if bytes.Equal(local.GetDigest(), remote.GetDigest()) {
			first = mid + 1
		} else {
			last = mid
		}
	}

	return first, nil
}

func (o *Observer) reportMismatch(ctx context.Context, id uint64, cursor *peerCursor, first, last, applied uint64, reason string) {
	key := fmt.Sprintf("%d:%d", first, last)
	if cursor.lastMismatch == key {
		return
	}
	cursor.lastMismatch = key
	o.divergences.Add(ctx, 1, metric.WithAttributes(attribute.String("peer_id", strconv.FormatUint(id, 10))))
	o.logger.WithFields(map[string]any{
		"peer_id": id, "first_log_sequence": first, "last_log_sequence": last,
		"min_applied_index": applied, "reason": reason,
	}).Errorf("Persisted log divergence detected")
}
