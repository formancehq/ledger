package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/domain"
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

// beginResponseFrontier prevents a write from registering between the read's
// response and its ticket snapshot. The caller must start the RPC after this
// call and invoke the returned closure immediately after the complete response
// (including stream drain) is observed.
func (c *Checker) beginResponseFrontier() func() uint64 {
	c.dispatchMu.Lock()

	return func() uint64 {
		defer c.dispatchMu.Unlock()

		return c.ticketSeq.Load()
	}
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

	if obs.ambiguousCommit {
		// A maintenance rejection after an ambiguous attempt does not determine
		// whether that attempt committed. Keep the original bulk as an optional
		// predecessor after its worker observation leaves inflight so validation
		// can still serialize through either outcome.
		if retainedTicket, retained := c.ambiguousMaintenanceEnableTicket(); !bulkEnablesMaintenance(obs.bulk) || !retained {
			c.ambiguousBulks[obs.ticket] = obs.bulk
		} else if obs.ticket < retainedTicket {
			delete(c.ambiguousBulks, retainedTicket)
			c.ambiguousBulks[obs.ticket] = obs.bulk
		}
	}
	c.removeInflight(obs.ticket)
	defer c.tryDrain()

	// Transient gRPC errors leave the model untouched — the bulk
	// effectively didn't happen. Shutdown errors (ctx cancelled / deadline
	// from MODEL_MAX_SECONDS) are dropped the same way: the outcome is
	// unknown but we're tearing down, so there's nothing to validate.
	if obs.err != nil && ((internal.IsTransient(obs.err) && !internal.HasErrorReason(obs.err, domain.ErrReasonMaintenanceMode)) || isShutdownError(obs.err)) {
		dbg("TRANSIENT/SHUTDOWN SKIP: ledgers=%s kinds=%s meta=%s err=%v", bulkLedgers(obs.bulk), requestKinds(obs.bulk), bulkMeta(obs.bulk), obs.err)
		markObservationProcessed(obs)
		return
	}

	if obs.err != nil {
		// Failed bulk consumes no log sequence. Accept iff some serialization of
		// the in-flight bulks dispatched no later than this failure's observe
		// high-water reproduces the observed error (validateFailure).
		dbg("BULK ERR: ledgers=%s kinds=%s meta=%s err=%v", bulkLedgers(obs.bulk), requestKinds(obs.bulk), bulkMeta(obs.bulk), obs.err)
		c.validateFailure(obs.observeTicket, obs.bulk, obs.err)
		if internal.HasErrorReason(obs.err, domain.ErrReasonMaintenanceMode) {
			emitCoverage(true, coverageMaintenanceMessage, internal.Details{}, coverageHit)
		}
		markModelOutcomeVerified()
		markObservationProcessed(obs)
		return
	}

	minSeq := minLogSequence(obs.resp.GetLogs())
	if minSeq == 0 {
		// Success with no committed log is impossible under the model.
		c.validateEmptyCommit(obs.bulk)
		markModelOutcomeVerified()
		markObservationProcessed(obs)
		return
	}

	c.insertPending(&pendingObservation{minSeq: minSeq, obs: obs})
}

// Multiple optional maintenance enables are state-equivalent until the
// recovery disable. Coalescing them keeps the candidate-search capacity bound
// while preserving both possible maintenance states. Caller holds c.mu.
func (c *Checker) ambiguousMaintenanceEnableTicket() (uint64, bool) {
	for ticket, bulk := range c.ambiguousBulks {
		if bulkEnablesMaintenance(bulk) {
			return ticket, true
		}
	}

	return 0, false
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
		if bulkDisablesMaintenance(head.obs.bulk) {
			for ticket, bulk := range c.ambiguousBulks {
				// A committed disable subsumes an optional earlier enable, but it
				// does not subsume an ambiguously committed business effect.
				if ticket <= head.obs.ticket && bulkEnablesMaintenance(bulk) {
					delete(c.ambiguousBulks, ticket)
				}
			}
		}
		markModelOutcomeVerified()
		markObservationProcessed(head.obs)
	}
}

func markObservationProcessed(obs observation) {
	if obs.processed != nil {
		close(obs.processed)
	}
}

// markModelOutcomeVerified is the report-visible proof that a definitive
// server result reached a model validation path. Keep this separate from setup
// assertions: setup runs before Checker exists and cannot establish conformance.
func markModelOutcomeVerified() {
	assert.Reachable("singleton_driver_model: model outcome verified", internal.Details{})
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

// Largest Log.Sequence in logs, or 0 if none.
func maxLogSequence(logs []*commonpb.Log) uint64 {
	var max uint64
	for _, l := range logs {
		if s := l.GetSequence(); s > max {
			max = s
		}
	}
	return max
}
