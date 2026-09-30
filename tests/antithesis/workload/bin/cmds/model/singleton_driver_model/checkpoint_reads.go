package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Checkpoint data is compared only with the drained state captured at creation.
// Live candidate states may explain deletion, never different business data.
func checkpointAccountReadMatches(state oracle.GlobalState, ledger, address string, account *commonpb.Account, found, collapsed bool) bool {
	ls := state.Ledger(ledger)
	if !found {
		return !modelKnowsAccount(ls, address)
	}

	return account != nil && account.GetAddress() == address && accountMatchesCollapsed(ls, address, account, collapsed)
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

// Keep both clients and the Raft identity paired. Bound a read on a down
// replica so it cannot indefinitely hold the model's ordered drain gate.
func runCheckpointRead(ctx context.Context, node *internal.PerNodeConn, c *Checker) {
	bucket := node.Bucket
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
	choice := internal.Rand().Uint64() % 10
	if choice == 0 || id == 0 {
		runCheckpointListRead(readCtx, node, c)

		return
	}
	if choice == 1 {
		runCheckpointScheduleRead(readCtx, node, c)

		return
	}
	var err error
	var matches bool
	var concrete bool
	var maxTicket uint64
	var ledger string
	details := internal.Details{"checkpoint": id, "readKind": choice}
	switch choice {
	case 2:
		var address string
		var ok bool
		ledger, address, _, _, _, ok = pickReadTarget(frozen, ledgerNames)
		if !ok {
			return
		}
		// A frozen store folds colours the same way a live one does, so the
		// option is rolled here too rather than only on the live read.
		collapsed := oneIn(3)

		var account *commonpb.Account
		account, err = bucket.GetAccount(readCtx, &servicepb.GetAccountRequest{
			Ledger:         ledger,
			Address:        address,
			CheckpointId:   id,
			CollapseColors: collapsed,
		})
		maxTicket = c.ticketSeq.Load()
		details["ledger"], details["address"], details["returned"] = ledger, address, account
		details["collapseColors"] = collapsed
		matches = checkpointAccountReadMatches(frozen, ledger, address, account, err == nil, collapsed)
		concrete = err == nil && account != nil && modelKnowsAccount(frozen.Ledger(ledger), address)
	case 3:
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
	case 7:
		if len(ledgerNames) == 0 {
			return
		}

		// A frozen store answers the boundaries it froze, exactly.
		ledger = ledgerNames[internal.Rand().Uint64()%uint64(len(ledgerNames))]

		var stats *commonpb.LedgerStats
		stats, err = bucket.GetLedgerStats(readCtx, &servicepb.GetLedgerStatsRequest{Ledger: ledger, CheckpointId: id})
		maxTicket = c.ticketSeq.Load()
		frozenLS := frozen.Ledger(ledger)
		details["ledger"], details["returned"] = ledger, stats
		matches = err == nil &&
			stats.GetTransactionCount() == uint64(frozenLS.Txs().Len()) &&
			stats.GetLogCount() == uint64(len(frozenLS.LogDates()))
		concrete = err == nil && stats.GetLogCount() > 0
	case 8:
		target, learned, ok := pickFrozenLogSequence(frozen, ledgerNames)
		if !ok {
			return
		}

		ledger = target.ledger

		var log *commonpb.Log
		log, err = bucket.GetLog(readCtx, &servicepb.GetLogRequest{Sequence: target.sequence, CheckpointId: id})
		maxTicket = c.ticketSeq.Load()
		details["ledger"], details["sequence"], details["learned"] = ledger, target.sequence, learned

		if !learned {
			// A sequence the frozen store never held must resolve NotFound.
			matches = err != nil && status.Code(err) == codes.NotFound
			concrete = matches
		} else {
			rows := serverLogRows([]*commonpb.Log{log})
			matches = err == nil && len(rows) == 1 && committedLogMatches(frozen.Ledger(ledger), target, rows[0])
			concrete = matches
		}
	case 6:
		if len(ledgerNames) == 0 {
			return
		}

		ledger = ledgerNames[internal.Rand().Uint64()%uint64(len(ledgerNames))]
		// A nil filter keeps the frozen fold clear of index readiness: every cell
		// the checkpoint holds is required by its own primary-store state.
		opts := aggOptions{collapseColors: oneIn(3), useMaxPrecision: oneIn(4), groupByPrefixes: genGroupPrefixes()}

		var res *commonpb.AggregateResult
		res, err = bucket.AggregateVolumes(readCtx, &servicepb.AggregateVolumesRequest{
			Ledger:          ledger,
			CheckpointId:    id,
			CollapseColors:  opts.collapseColors,
			UseMaxPrecision: opts.useMaxPrecision,
			GroupByPrefixes: opts.groupByPrefixes,
		})
		maxTicket = c.ticketSeq.Load()
		details["ledger"], details["prefixes"] = ledger, strings.Join(opts.groupByPrefixes, ",")
		matches, concrete = checkpointAggregateMatches(frozen.Ledger(ledger), opts, res, err == nil)

		if matches && err == nil {
			details["returned"] = describeCheckpointAggregate(opts, res)
		}
	case 9:
		// A frozen fleet cannot move under a resumed page, so the window, its
		// resume token and every row's whole record are predicted exactly.
		pageSize := 1 + int(internal.Rand().Uint64()%16)
		reverse := percentChance(50)

		var cursor string
		if oneIn(2) {
			cursor = random.RandomChoice(c.ledgerNamesSnapshot())
		}

		stream, streamErr := bucket.ListLedgers(readCtx, &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{
			Read:     &commonpb.ReadOptions{CheckpointId: id},
			PageSize: uint32(pageSize),
			Cursor:   cursor,
			Reverse:  reverse,
		}})
		err = streamErr

		var rows []*commonpb.LedgerInfo
		if err == nil {
			rows, err = drainStream(stream)
		}

		var next string
		if err == nil {
			next = nextCursorOf(stream)
		}

		maxTicket = c.ticketSeq.Load()

		names := make([]string, len(rows))
		for i, row := range rows {
			names[i] = row.GetName()
		}

		want, more := ledgerWindow(frozen.LiveLedgers(), cursor, pageSize, reverse)
		details["cursor"], details["pageSize"], details["reverse"] = cursor, pageSize, reverse
		details["expectedLedgers"], details["returned"], details["nextCursor"] = want, names, next
		matches = err == nil && slices.Equal(names, want) &&
			nextCursorLegal(next, more, lastLedgerKey(names), len(names), pageSize)
		concrete = err == nil && len(rows) > 0

		if matches {
			for _, row := range rows {
				if ledgerInfoStructureViolation(row) != "" || !ledgerInfoMatches(frozen, row) {
					matches = false

					break
				}
			}
		}

		if matches && len(rows) > 0 {
			// Coverage: the fleet a checkpoint froze was listed back whole. The
			// other arms read one ledger's contents; this is the only one whose
			// subject is the membership itself.
			assert.Reachable("singleton_driver_model: coverage a checkpoint served the frozen fleet listing", internal.Details{"checkpoint": id, "rows": len(rows)})
		}
	default:
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
			// The frozen state cannot move under a resumed page, so a cursor here
			// is the one window the model predicts exactly.
			var cursor string
			if oneIn(2) {
				cursor = poolAddress()
			}

			options.Cursor = cursor
			details["cursor"] = cursor

			stream, streamErr := bucket.ListAccounts(readCtx, &servicepb.ListAccountsRequest{Ledger: ledger, Options: options})
			err = streamErr
			var rows []*commonpb.Account
			if err == nil {
				rows, err = drainStream(stream)
			}
			var next string
			if err == nil {
				next = nextCursorOf(stream)
			}
			maxTicket = c.ticketSeq.Load()
			want, more := checkpointAccountWindow(frozen.Ledger(ledger), cursor, pageSize, reverse)
			details["expectedAddresses"], details["returned"], details["nextCursor"] = want, rows, next
			matches = err == nil && len(rows) == len(want) && nextCursorLegal(next, more, lastAccountKey(rows), len(rows), pageSize)
			concrete = err == nil && len(rows) > 0
			if matches {
				for i, address := range want {
					if !checkpointAccountReadMatches(frozen, ledger, address, rows[i], true, false) {
						matches = false

						break
					}
				}
			}
		} else {
			var (
				cursor  string
				afterID uint64
			)

			if oneIn(2) {
				afterID = 1 + internal.Rand().Uint64()%256
				cursor = strconv.FormatUint(afterID, 10)
			}

			options.Cursor = cursor
			details["cursor"] = cursor

			stream, streamErr := bucket.ListTransactions(readCtx, &servicepb.ListTransactionsRequest{Ledger: ledger, Options: options})
			err = streamErr
			var rows []*commonpb.Transaction
			if err == nil {
				rows, err = drainStream(stream)
			}
			var next string
			if err == nil {
				next = nextCursorOf(stream)
			}
			maxTicket = c.ticketSeq.Load()
			details["returned"], details["nextCursor"] = rows, next
			matches = err == nil && txWindowMatches(frozen.Ledger(ledger), nil, afterID, pageSize, reverse, rows, next)
			concrete = err == nil && len(rows) > 0
		}
	}
	if err != nil && (internal.IsTransient(err) || isShutdownError(err)) {
		return
	}
	frozenMatches := matches
	matches = c.checkpointLedgerReadOutcomeMatches(id, maxTicket, ledger, frozenMatches, err)
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
		return predictedCheckpointLedgerMatches(state, ledger, info)
	})
	c.mu.Unlock()
	if !matches {
		assert.Unreachable("singleton_driver_model: predicted checkpoint read outside creation model", internal.Details{"checkpoint": checkpointID, "ledger": ledger})
	}
}

func predictedCheckpointLedgerMatches(state oracle.GlobalState, ledger string, info *commonpb.LedgerInfo) bool {
	return info.GetName() == ledger && ledgerInfoMatches(state, info)
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

func runCheckpointListRead(ctx context.Context, node *internal.PerNodeConn, c *Checker) {
	c.mu.Lock()
	sequences := make(map[uint64]uint64, len(c.checkpoints))
	for id, snapshot := range c.checkpoints {
		sequences[id] = snapshot.maxSequence
	}
	c.mu.Unlock()
	responseFrontier := c.beginResponseFrontier()
	response, err := readCheckpointRegistry(ctx, node)
	maxTicket := responseFrontier()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
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
		assert.Unreachable("singleton_driver_model: checkpoint list outside model", internal.Details{"ids": ids, "nodeID": node.NodeID, "address": node.Addr})

		return
	}
	noteCheckpointCoverage(checkpointListCoverage)
}

func runCheckpointScheduleRead(ctx context.Context, node *internal.PerNodeConn, c *Checker) {
	responseFrontier := c.beginResponseFrontier()
	response, err := readCheckpointSchedule(ctx, node)
	maxTicket := responseFrontier()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		assert.Unreachable("singleton_driver_model: checkpoint schedule returned unexpected error", internal.Details{"error": err.Error()})

		return
	}
	if !c.matchesModel(maxTicket, "CHECKPOINTSCHEDULE", func(base oracle.GlobalState) bool { return response.GetCron() == base.QueryCheckpointSchedule() }) {
		assert.Unreachable("singleton_driver_model: checkpoint schedule outside model", internal.Details{"cron": response.GetCron(), "nodeID": node.NodeID, "address": node.Addr})

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

// checkpointAccountWindow is the unfiltered accounts page a frozen checkpoint
// must serve, with the verdict on whether a further row is waiting. The frozen
// state cannot move, so both are exact.
func checkpointAccountWindow(ls oracle.LedgerState, cursor string, pageSize int, reverse bool) ([]string, cursorMore) {
	want := accountWindow(ls, nil, cursor, pageSize+1, reverse)
	if len(want) > pageSize {
		return want[:pageSize], cursorRequired
	}

	return want, cursorForbidden
}

// checkpointAggregateMatches compares an aggregate served from a frozen
// checkpoint against that checkpoint's own state. The frozen state cannot move,
// so the fold is exact; concrete reports whether the answer carried any total,
// which is what makes the read coverage-worthy.
func checkpointAggregateMatches(ls oracle.LedgerState, opts aggOptions, res *commonpb.AggregateResult, served bool) (matches, concrete bool) {
	if !served {
		return false, false
	}

	if len(opts.groupByPrefixes) > 0 {
		if len(res.GetVolumes()) != 0 {
			return false, false
		}

		groups, ok := serverAggregateGroups(res)
		if !ok {
			return false, false
		}

		for _, g := range groups {
			if len(g.sums) > 0 {
				concrete = true
			}
		}

		return aggGroupsEqual(modelAggregateGroups(ls, nil, opts), groups), concrete
	}

	if len(res.GetGroups()) != 0 {
		return false, false
	}

	sums, ok := serverAggregate(res)
	if !ok {
		return false, false
	}

	return aggEqual(modelAggregate(ls, nil, opts), sums), len(sums) > 0
}

// describeCheckpointAggregate renders whichever arm the request asked for.
func describeCheckpointAggregate(opts aggOptions, res *commonpb.AggregateResult) string {
	if len(opts.groupByPrefixes) > 0 {
		groups, _ := serverAggregateGroups(res)

		return renderAggGroups(groups)
	}

	sums, _ := serverAggregate(res)

	return renderAgg(sums)
}

// checkpointLedgerReadOutcomeMatches is checkpointReadOutcomeMatches for a
// ledger-scoped read: a NotFound is equally explained by the ledger being
// deleted, which no frozen checkpoint keeps serving.
func (c *Checker) checkpointLedgerReadOutcomeMatches(id, maxTicket uint64, ledger string, frozenMatches bool, err error) bool {
	if c.checkpointReadOutcomeMatches(id, maxTicket, frozenMatches, err) {
		return true
	}

	if ledger == "" || !checkpointNotFound(err) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	matches := false
	c.candidateBases(maxTicket, func(base oracle.GlobalState) bool {
		matches = !ledgerIsLive(base, ledger)

		return matches
	})

	return matches
}

// pickFrozenLogSequence chooses a global sequence a frozen checkpoint holds a
// log for, or — one read in eight — one past everything it froze.
func pickFrozenLogSequence(frozen oracle.GlobalState, ledgerNames []string) (target committedLogTarget, learned, ok bool) {
	var (
		known  []committedLogTarget
		maxSeq uint64
	)

	for _, ledger := range ledgerNames {
		for _, row := range frozen.Ledger(ledger).LogRows() {
			if row.Sequence == 0 {
				continue
			}

			known = append(known, committedLogTarget{ledger: ledger, id: row.ID, sequence: row.Sequence})
			maxSeq = max(maxSeq, row.Sequence)
		}
	}

	if len(known) == 0 {
		return committedLogTarget{}, false, false
	}

	if oneIn(8) {
		return committedLogTarget{sequence: maxSeq + unassignedSeqSlack}, false, true
	}

	return known[internal.Rand().Intn(len(known))], true, true
}
