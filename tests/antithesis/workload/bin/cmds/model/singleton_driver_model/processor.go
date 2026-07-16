package main

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// registerInflight reserves a ticket and records the bulk. Must run BEFORE the
// Apply: the ticket is the dispatch order tryDrain relies on, and the bulk is
// what the serialization search (candidateBases) folds. Caller holds c.mu.
func (c *Checker) registerInflight(bulk oracle.Bulk) uint64 {
	t := c.ticketSeq.Add(1)
	c.inflight[t] = bulk

	return t
}

// Drops a ticket. Caller holds c.mu.
func (c *Checker) removeInflight(ticket uint64) {
	delete(c.inflight, ticket)
}

// registerRead reserves a ticket for an outstanding read. Holding it gates
// draining (see tryDrain), so the read needs no drain-race skip. Caller holds c.mu.
func (c *Checker) registerRead() uint64 {
	t := c.ticketSeq.Add(1)
	c.reads[t] = struct{}{}

	return t
}

// finishRead drops an outstanding read and resumes any draining it held back.
func (c *Checker) finishRead(ticket uint64) {
	c.mu.Lock()
	delete(c.reads, ticket)
	c.tryDrain()
	c.mu.Unlock()
}

// Smallest ticket across all outstanding operations (in-flight bulks and
// outstanding reads); empty=true when there are none. See tryDrain for the gate.
// Caller holds c.mu.
func (c *Checker) earliestOutstanding() (uint64, bool) {
	var min uint64
	found := false
	consider := func(t uint64) {
		if !found || t < min {
			min = t
			found = true
		}
	}

	for ticket := range c.inflight {
		consider(ticket)
	}
	for ticket := range c.reads {
		consider(ticket)
	}

	return min, !found
}

// Response handler. Failures validate immediately against state + in-flight
// effects; successes buffer by minSeq and drain in order (see tryDrain).
func (c *Checker) runProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case obs, ok := <-c.incoming:
			if !ok {
				return
			}
			c.handleObservation(obs)
		}
	}
}

func (c *Checker) handleObservation(obs observation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeInflight(obs.ticket)

	// Transient gRPC errors leave the model untouched — the bulk
	// effectively didn't happen. Shutdown errors (ctx cancelled / deadline
	// from MODEL_MAX_SECONDS) are dropped the same way: the outcome is
	// unknown but we're tearing down, so there's nothing to validate.
	if obs.err != nil && (internal.IsTransient(obs.err) || isShutdownError(obs.err)) {
		dbg("TRANSIENT/SHUTDOWN SKIP: ledgers=%s kinds=%s meta=%s err=%v", bulkLedgers(obs.bulk), requestKinds(obs.bulk), bulkMeta(obs.bulk), obs.err)
		return
	}

	if obs.err != nil {
		// Failed bulk consumes no log sequence. Accept iff some serialization of
		// the in-flight bulks dispatched no later than this failure's observe
		// high-water reproduces the observed error (validateFailure).
		dbg("BULK ERR: ledgers=%s kinds=%s meta=%s err=%v", bulkLedgers(obs.bulk), requestKinds(obs.bulk), bulkMeta(obs.bulk), obs.err)
		c.validateFailure(obs.observeTicket, obs.bulk, obs.err)
		return
	}

	minSeq := minLogSequence(obs.resp.GetLogs())
	if minSeq == 0 {
		// Success with no committed log is impossible under the model.
		c.validateEmptyCommit(obs.bulk)
		return
	}

	c.insertPending(&pendingObservation{minSeq: minSeq, obs: obs})
	c.tryDrain()
}

// Drains buffered observations in log-sequence order while safe: the head drains
// only once every outstanding operation (in-flight bulk or read) has a ticket
// greater than the head's observeTicket — i.e. was dispatched after the head was
// observed, so a bulk committed after it (can't precede it) and a read saw it.
// That gate is what lets failures and reads validate against the model with no
// skip. Caller holds c.mu.
func (c *Checker) tryDrain() {
	for len(c.pending) > 0 {
		head := c.pending[0]
		minTicket, empty := c.earliestOutstanding()
		if !empty && minTicket <= head.obs.observeTicket {
			return
		}

		c.pending = c.pending[1:]
		c.validateBulkSuccess(head.obs.bulk, head.obs.resp)
	}
}

// Inserts into c.pending, kept sorted ascending by minSeq. Caller holds c.mu.
func (c *Checker) insertPending(entry *pendingObservation) {
	i := 0
	for i < len(c.pending) && c.pending[i].minSeq < entry.minSeq {
		i++
	}

	c.pending = append(c.pending, nil)
	copy(c.pending[i+1:], c.pending[i:])
	c.pending[i] = entry
}

// Smallest non-zero Log.Sequence in logs, or 0 if none.
func minLogSequence(logs []*commonpb.Log) uint64 {
	var min uint64
	for _, l := range logs {
		s := l.GetSequence()
		if s == 0 {
			continue
		}
		if min == 0 || s < min {
			min = s
		}
	}
	return min
}
