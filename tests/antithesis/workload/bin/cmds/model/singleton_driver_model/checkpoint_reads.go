package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Checkpoint data is compared only with the drained state captured at creation.
// Live candidate states may explain deletion, never different business data.
func checkpointAccountReadMatches(state oracle.GlobalState, ledger, address string, account *commonpb.Account, found bool) bool {
	ls := state.Ledger(ledger)
	if !found {
		return !modelKnowsAccount(ls, address)
	}
	return account != nil && account.GetAddress() == address && accountMatches(ls, address, account)
}

func checkpointTransactionReadMatches(state oracle.GlobalState, ledger string, id uint64, transaction *commonpb.Transaction, found bool) bool {
	txs := state.Ledger(ledger).Txs()
	if id == 0 || id > uint64(txs.Len()) {
		return !found
	}
	return found && transaction != nil && txRecordMatches(txs.Get(int(id-1)), transaction)
}

// The server maps commonpb.NotFoundError to a typed gRPC status without
// ErrorInfo details (server.go). Text containing "not found" is never evidence.
func checkpointNotFound(err error) bool {
	statusValue, ok := status.FromError(err)
	return ok && statusValue.Code() == codes.NotFound
}

// bucket and cluster must address the same node. Bound a read on a down
// replica so it cannot indefinitely hold the model's ordered drain gate.
func runCheckpointRead(ctx context.Context, bucket servicepb.BucketServiceClient, cluster clusterpb.ClusterServiceClient, c *Checker) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c.mu.Lock()
	if c.paused {
		c.mu.Unlock()
		return
	}
	readID := c.registerRead()
	id, frozen, deleted := c.pickCheckpointReadTarget()
	c.mu.Unlock()
	defer c.finishRead(readID)
	ledgerNames := liveLedgerNames(frozen, c.ledgerNamesSnapshot())
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	choice := internal.Rand().Uint64() % 6
	if choice == 0 || id == 0 {
		runCheckpointListRead(readCtx, bucket, cluster, c)
		return
	}
	if choice == 1 {
		runCheckpointScheduleRead(readCtx, bucket, cluster, c)
		return
	}
	var err error
	var matches bool
	var concrete bool
	var maxTicket uint64
	var ledger string
	details := internal.Details{"checkpoint": id, "readKind": choice}
	if choice == 2 {
		var address string
		var ok bool
		ledger, address, _, _, _, ok = pickReadTarget(frozen, ledgerNames)
		if !ok {
			return
		}
		var account *commonpb.Account
		account, err = bucket.GetAccount(readCtx, &servicepb.GetAccountRequest{Ledger: ledger, Address: address, CheckpointId: id})
		maxTicket = c.ticketSeq.Load()
		details["ledger"], details["address"], details["returned"] = ledger, address, account
		matches = checkpointAccountReadMatches(frozen, ledger, address, account, err == nil)
		concrete = err == nil && account != nil && modelKnowsAccount(frozen.Ledger(ledger), address)
	} else if choice == 3 {
		var txID uint64
		var ok bool
		ledger, txID, _, ok = pickTransactionID(frozen, ledgerNames)
		if !ok {
			return
		}
		var response *servicepb.GetTransactionResponse
		response, err = bucket.GetTransaction(readCtx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: txID, CheckpointId: id})
		maxTicket = c.ticketSeq.Load()
		details["ledger"], details["transactionId"], details["returned"] = ledger, txID, response.GetTransaction()
		matches = checkpointTransactionReadMatches(frozen, ledger, txID, response.GetTransaction(), err == nil)
		concrete = err == nil && response.GetTransaction() != nil
	} else {
		if len(ledgerNames) == 0 {
			return
		}
		ledger = ledgerNames[internal.Rand().Uint64()%uint64(len(ledgerNames))]
		pageSize := 1 + int(internal.Rand().Uint64()%16)
		reverse := percentChance(50)
		details["ledger"], details["pageSize"], details["reverse"] = ledger, pageSize, reverse
		options := &commonpb.ListOptions{Read: &commonpb.ReadOptions{CheckpointId: id}, PageSize: uint32(pageSize), Reverse: reverse}
		// Nil filters deliberately avoid asynchronous index readiness: every row
		// in this page is required by the frozen primary-store state.
		if choice == 4 {
			stream, streamErr := bucket.ListAccounts(readCtx, &servicepb.ListAccountsRequest{Ledger: ledger, Options: options})
			err = streamErr
			var rows []*commonpb.Account
			if err == nil {
				rows, err = drainStream(stream)
			}
			maxTicket = c.ticketSeq.Load()
			want := accountWindow(frozen.Ledger(ledger), nil, "", pageSize, reverse)
			details["expectedAddresses"], details["returned"] = want, rows
			matches = err == nil && len(rows) == len(want)
			concrete = err == nil && len(rows) > 0
			if matches {
				for i, address := range want {
					if !checkpointAccountReadMatches(frozen, ledger, address, rows[i], true) {
						matches = false
						break
					}
				}
			}
		} else {
			stream, streamErr := bucket.ListTransactions(readCtx, &servicepb.ListTransactionsRequest{Ledger: ledger, Options: options})
			err = streamErr
			var rows []*commonpb.Transaction
			if err == nil {
				rows, err = drainStream(stream)
			}
			maxTicket = c.ticketSeq.Load()
			details["returned"] = rows
			matches = err == nil && txWindowMatches(frozen.Ledger(ledger), nil, 0, pageSize, reverse, rows)
			concrete = err == nil && len(rows) > 0
		}
	}
	if err != nil && (internal.IsTransient(err) || isShutdownError(err)) {
		return
	}
	frozenMatches := matches
	matches = c.checkpointReadOutcomeMatches(id, maxTicket, frozenMatches, err)
	if !matches {
		details["error"] = fmt.Sprint(err)
		assert.Unreachable("singleton_driver_model: checkpoint read outside frozen model", details)
		return
	}
	// An absent entity can return NotFound even from a live checkpoint.
	// Count deletion coverage only when the frozen result cannot explain it.
	if deleted && !frozenMatches && checkpointNotFound(err) {
		noteCheckpointCoverage(checkpointDeletedReadCoverage)
	}
	if concrete {
		noteCheckpointCoverage(checkpointReadCoverage)
	}
}

// runPredictedCheckpointRead targets the ID assigned by the matching in-flight
// create. NotFound is valid before commit; success must expose the exact model
// state immediately before that create in a legal serialization.
func runPredictedCheckpointRead(ctx context.Context, bucket servicepb.BucketServiceClient, c *Checker, checkpointID uint64, start <-chan struct{}, registered chan<- struct{}) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	close(registered)
	defer c.finishRead(readID)

	select {
	case <-ctx.Done():
		return
	case <-start:
	}

	ledgerNames := c.liveLedgerNamesSnapshot()
	if len(ledgerNames) == 0 {
		return
	}
	ledger := ledgerNames[0]
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	info, err := bucket.GetLedger(readCtx, &servicepb.GetLedgerRequest{
		Ledger: ledger,
		Read:   &commonpb.ReadOptions{CheckpointId: checkpointID},
	})
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if checkpointNotFound(err) && c.checkpointReadOutcomeMatches(checkpointID, maxTicket, false, err) {
			return
		}
		assert.Unreachable("singleton_driver_model: predicted checkpoint read returned unexpected error", internal.Details{"checkpoint": checkpointID, "error": err.Error()})

		return
	}

	c.mu.Lock()
	matches := c.checkpointCreationMatches(maxTicket, checkpointID, func(state oracle.GlobalState) bool {
		ls := state.Ledger(ledger)

		return chartMatches(ls, info.GetAccountTypes()) && ledgerMetaMatches(ls, info.GetMetadata())
	})
	c.mu.Unlock()
	if !matches {
		assert.Unreachable("singleton_driver_model: predicted checkpoint read outside creation model", internal.Details{"checkpoint": checkpointID, "ledger": ledger})
	}
}

// pickCheckpointReadTarget samples uniformly from every retained snapshot.
// Whether the ID is live or deleted is derived from its owning registry and is
// used only to credit a definitive post-deletion NotFound observation.
// Caller holds c.mu.
func (c *Checker) pickCheckpointReadTarget() (uint64, oracle.GlobalState, bool) {
	ids := make([]uint64, 0, len(c.checkpoints)+len(c.deletedCheckpoints))
	for id := range c.checkpoints {
		ids = append(ids, id)
	}
	ids = append(ids, c.deletedCheckpoints...)
	if len(ids) == 0 {
		return 0, oracle.GlobalState{}, false
	}
	slices.Sort(ids)
	id := ids[internal.Rand().Uint64()%uint64(len(ids))]
	if snapshot, ok := c.checkpoints[id]; ok {
		return id, snapshot.state, false
	}

	return id, c.deletedCheckpointSnapshots[id].state, true
}

func runCheckpointListRead(ctx context.Context, bucket servicepb.BucketServiceClient, client clusterpb.ClusterServiceClient, c *Checker) {
	c.mu.Lock()
	sequences := make(map[uint64]uint64, len(c.checkpoints))
	for id, snapshot := range c.checkpoints {
		sequences[id] = snapshot.maxSequence
	}
	c.mu.Unlock()
	ledgerNames := c.liveLedgerNamesSnapshot()
	if len(ledgerNames) == 0 {
		return
	}
	ledger := ledgerNames[0]
	response, err := readCheckpointRegistry(ctx, bucket, client, ledger)
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if checkpointNotFound(err) && c.matchesModel(maxTicket, "CHECKPOINTLISTLEDGER", func(base oracle.GlobalState) bool {
			return checkpointLedgerUnavailable(base, ledger)
		}) {
			return
		}
		assert.Unreachable("singleton_driver_model: checkpoint list returned unexpected error", internal.Details{"error": err.Error()})
		return
	}
	ids := make([]uint64, 0, len(response.GetCheckpoints()))
	for _, checkpoint := range response.GetCheckpoints() {
		ids = append(ids, checkpoint.GetCheckpointId())
		if sequence, known := sequences[checkpoint.GetCheckpointId()]; known && checkpoint.GetMaxSequence() != sequence {
			assert.Unreachable("singleton_driver_model: checkpoint list sequence outside captured model", internal.Details{"checkpoint": checkpoint.GetCheckpointId(), "expected": sequence, "actual": checkpoint.GetMaxSequence()})
			return
		}
	}
	slices.Sort(ids)
	if !c.matchesModel(maxTicket, "CHECKPOINTLIST", func(base oracle.GlobalState) bool { return slices.Equal(ids, base.QueryCheckpointIDs()) }) {
		assert.Unreachable("singleton_driver_model: checkpoint list outside model", internal.Details{"ids": ids})
		return
	}
	noteCheckpointCoverage(checkpointListCoverage)
}

func runCheckpointScheduleRead(ctx context.Context, bucket servicepb.BucketServiceClient, client clusterpb.ClusterServiceClient, c *Checker) {
	ledgerNames := c.liveLedgerNamesSnapshot()
	if len(ledgerNames) == 0 {
		return
	}
	ledger := ledgerNames[0]
	response, err := readCheckpointSchedule(ctx, bucket, client, ledger)
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if checkpointNotFound(err) && c.matchesModel(maxTicket, "CHECKPOINTSCHEDULELEDGER", func(base oracle.GlobalState) bool {
			return checkpointLedgerUnavailable(base, ledger)
		}) {
			return
		}
		assert.Unreachable("singleton_driver_model: checkpoint schedule returned unexpected error", internal.Details{"error": err.Error()})
		return
	}
	if !c.matchesModel(maxTicket, "CHECKPOINTSCHEDULE", func(base oracle.GlobalState) bool { return response.GetCron() == base.QueryCheckpointSchedule() }) {
		assert.Unreachable("singleton_driver_model: checkpoint schedule outside model", internal.Details{"cron": response.GetCron()})
		return
	}
	noteCheckpointCoverage(checkpointScheduleReadCoverage)
}

// checkpointReadOutcomeMatches never substitutes current business data for the
// frozen result, including expected entity absence. Candidate states can
// explain only a missing checkpoint.
func (c *Checker) checkpointReadOutcomeMatches(id, maxTicket uint64, frozenMatches bool, err error) bool {
	if err == nil {
		return frozenMatches
	}
	if !checkpointNotFound(err) {
		return false
	}
	if frozenMatches {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	matches := false
	c.candidateBases(maxTicket, func(base oracle.GlobalState) bool {
		matches = !base.QueryCheckpointExists(id)
		return matches
	})
	return matches
}

func checkpointLedgerUnavailable(state oracle.GlobalState, ledger string) bool {
	lifecycle, exists := state.Lifecycle(ledger)
	if exists {
		return lifecycle.Deleted
	}
	_, exists = state.Ledgers()[ledger]
	return !exists
}
