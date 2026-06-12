package state

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// SentinelTracer accumulates diagnostic data during PrepareEntries in sentinel mode.
// It logs each entry as it is processed (proposal applied/rejected), emits
// lifecycle.SendEvent for Antithesis causality analysis, and accumulates
// volume details for a full dump only when a post-commit sentinel check fails.
//
// One instance is allocated per PrepareEntries invocation (and threaded through
// the WriteSet) so that a later batch's traces don't overwrite the entries the
// pipelined committer is about to dump on a sentinel failure.
type SentinelTracer struct {
	logger  logging.Logger
	entries []sentinelEntryTrace
}

// NewSentinelTracer creates a tracer scoped to one PrepareEntries call.
func NewSentinelTracer(logger logging.Logger) *SentinelTracer {
	return &SentinelTracer{logger: logger}
}

type sentinelVolumeTrace struct {
	Key          domain.VolumeKey
	CanonicalKey string // hex
	ID           string // hex
	OldInput     string
	OldOutput    string
	NewInput     string
	NewOutput    string
	Partition    string // "kept", "transient", "purged"
}

type sentinelEntryTrace struct {
	RaftIndex    uint64
	ProposalID   uint64
	OrderCount   int
	LedgerIDs    []uint32
	Volumes      []sentinelVolumeTrace
	CacheRotated bool
	Rejected     bool
	Error        string
	LogCount     int
}

// SkipEntry logs a skipped non-normal/empty entry.
func (t *SentinelTracer) SkipEntry(raftIndex uint64, entryType string, dataLen int) {
	t.logger.WithFields(map[string]any{
		"raftIndex": raftIndex,
		"entryType": entryType,
		"dataLen":   dataLen,
	}).Tracef("SENTINEL: skipping non-normal/empty entry")
}

// StartEntry begins tracing a new raft entry.
func (t *SentinelTracer) StartEntry(raftIndex, proposalID uint64, orderCount int) {
	t.entries = append(t.entries, sentinelEntryTrace{
		RaftIndex:  raftIndex,
		ProposalID: proposalID,
		OrderCount: orderCount,
	})
}

// RecordApplied records a successfully applied proposal and logs it.
func (t *SentinelTracer) RecordApplied(ledgerIDs []uint32, logCount, volumeCount, purgedCount int) {
	if len(t.entries) == 0 {
		return
	}

	e := &t.entries[len(t.entries)-1]
	e.LedgerIDs = ledgerIDs
	e.LogCount = logCount

	t.logger.WithFields(map[string]any{
		"raftIndex":     e.RaftIndex,
		"proposalID":    e.ProposalID,
		"orderCount":    e.OrderCount,
		"ledgerIDs":     ledgerIDs,
		"logCount":      logCount,
		"volumeUpdates": volumeCount,
		"purgedVolumes": purgedCount,
	}).Tracef("SENTINEL: proposal applied")

	lifecycle.SendEvent("proposal_applied", map[string]any{
		"raftIndex":     e.RaftIndex,
		"proposalID":    e.ProposalID,
		"orderCount":    e.OrderCount,
		"ledgerIDs":     ledgerIDs,
		"logCount":      logCount,
		"volumeUpdates": volumeCount,
		"purgedVolumes": purgedCount,
	})
}

// RecordRejected records a rejected proposal and logs it.
func (t *SentinelTracer) RecordRejected(errMsg string) {
	if len(t.entries) == 0 {
		return
	}

	e := &t.entries[len(t.entries)-1]
	e.Rejected = true
	e.Error = errMsg

	t.logger.WithFields(map[string]any{
		"raftIndex":  e.RaftIndex,
		"proposalID": e.ProposalID,
		"error":      errMsg,
		"orderCount": e.OrderCount,
	}).Tracef("SENTINEL: proposal rejected (business error)")

	lifecycle.SendEvent("proposal_rejected", map[string]any{
		"raftIndex":  e.RaftIndex,
		"proposalID": e.ProposalID,
		"error":      errMsg,
		"orderCount": e.OrderCount,
	})
}

// SetCacheRotated marks the current entry as triggering a cache rotation.
func (t *SentinelTracer) SetCacheRotated() {
	if len(t.entries) == 0 {
		return
	}

	e := &t.entries[len(t.entries)-1]
	e.CacheRotated = true
}

// TraceVolumeUpdates records volume updates with their partition classification.
// Emits a SendEvent per entry for causality analysis with aggregated counts.
func (t *SentinelTracer) TraceVolumeUpdates(
	kept []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	transient []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	purged []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) {
	if len(t.entries) == 0 {
		return
	}

	e := &t.entries[len(t.entries)-1]

	for _, u := range kept {
		e.Volumes = append(e.Volumes, traceUpdate(u, "kept"))
	}

	for _, u := range transient {
		e.Volumes = append(e.Volumes, traceUpdate(u, "transient"))
	}

	for _, u := range purged {
		e.Volumes = append(e.Volumes, traceUpdate(u, "purged"))
	}

	lifecycle.SendEvent("volume_partition", map[string]any{
		"raftIndex": e.RaftIndex,
		"kept":      len(kept),
		"transient": len(transient),
		"purged":    len(purged),
	})
}

func traceUpdate(u attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], partition string) sentinelVolumeTrace {
	trace := sentinelVolumeTrace{
		Key:          u.Key,
		CanonicalKey: hex.EncodeToString(u.CanonicalKey),
		ID:           fmt.Sprintf("%x", u.ID),
		NewInput:     u.New.GetInput().ToBigInt().String(),
		NewOutput:    u.New.GetOutput().ToBigInt().String(),
		Partition:    partition,
	}

	if u.Old.IsDefined() && u.Old.Value() != nil {
		trace.OldInput = u.Old.Value().GetInput().ToBigInt().String()
		trace.OldOutput = u.Old.Value().GetOutput().ToBigInt().String()
	}

	return trace
}

// Dump logs the full trace to the logger. Called only on sentinel check failure.
func (t *SentinelTracer) Dump(logger logging.Logger) {
	if len(t.entries) == 0 {
		return
	}

	logger.Errorf("SENTINEL TRACE: dumping %d entries from failed batch", len(t.entries))

	for _, e := range t.entries {
		var status string
		if e.Rejected {
			status = "REJECTED: " + e.Error
		} else {
			status = "applied"
		}

		var volSummary string
		if len(e.Volumes) > 0 {
			parts := make([]string, 0, len(e.Volumes))
			for _, v := range e.Volumes {
				parts = append(parts, fmt.Sprintf(
					"%d/%s/%s[%s] key=%s id=%s old(%s,%s)→new(%s,%s)",
					v.Key.LedgerID, v.Key.Account, v.Key.Asset, v.Partition,
					v.CanonicalKey, v.ID,
					v.OldInput, v.OldOutput,
					v.NewInput, v.NewOutput,
				))
			}

			volSummary = strings.Join(parts, " | ")
		} else {
			volSummary = "(none)"
		}

		logger.WithFields(map[string]any{
			"raftIndex":    e.RaftIndex,
			"proposalID":   e.ProposalID,
			"orderCount":   e.OrderCount,
			"ledgerIDs":    e.LedgerIDs,
			"status":       status,
			"cacheRotated": e.CacheRotated,
			"volumes":      volSummary,
		}).Errorf("SENTINEL TRACE: entry")
	}
}
