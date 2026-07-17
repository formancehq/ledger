package usagebuilder

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

// ---------------------------------------------------------------------------
// Fixture-seeding harness
//
// processAuditEntries reads, per audit sequence, from the PRIMARY pebble store:
//   - the auditpb.AuditEntry             ([ZoneCold][SubColdAudit][seq BE8])
//   - its auditpb.AuditItem(s)           ([ZoneCold][SubColdAuditItem][seq BE8][idx BE4])
//   - the proposalpb.AppliedProposal     ([ZoneCold][SubColdAppliedProposal][seq BE8])
//   - the commonpb.LedgerLog per item    ([ZoneCold][SubColdLog][logSeq BE8]) via readLog
//
// The FSM write helpers that produce these rows (state.batch.go) are
// unexported, so we mirror their key layouts here with the DAL key builder —
// the same layouts query/audit.go, query/applied_proposal.go and query/log.go
// read back. Only structurally-sufficient fields are set: processAuditEntries
// does not verify the hash chain (only the checker does), so a minimal but
// valid (Sequence / Success / items referencing valid log sequences) fixture
// drives every path.
//
// NOTE: query.ReadAppliedProposal returns domain.ErrNotFound when the row is
// absent, which processAuditEntries treats as fatal — so every SUCCESS audit
// entry must have a matching AppliedProposal row (possibly empty).
// ---------------------------------------------------------------------------

func newUsageTestStores(t *testing.T) (*dal.Store, *usagestore.Store) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	primary, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = primary.Close() })

	usage, err := usagestore.New(t.TempDir(), logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = usage.Close() })

	return primary, usage
}

func usageColdAuditKey(seq uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutUint64(seq).
		Build()
}

func usageColdAuditItemKey(seq uint64, idx uint32) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).
		PutUint64(seq).
		PutUint32(idx).
		Build()
}

func usageColdAppliedProposalKey(seq uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).
		PutUint64(seq).
		Build()
}

func usageColdLogKey(seq uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutUint64(seq).
		Build()
}

// touchedVolume builds a (account, asset, color) volume identity tuple.
func touchedVolume(account, asset, color string) *commonpb.TouchedVolume {
	return &commonpb.TouchedVolume{Account: account, Asset: asset, Color: color}
}

// usagePosting builds a posting for the transaction payload — the posting
// count is derived from len(Transaction.Postings) on the produced log.
func usagePosting(source, destination, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Asset:       asset,
		Amount:      commonpb.NewUint256FromUint64(amount),
	}
}

// createdTxLog builds an Apply log carrying a CreatedTransaction plus the three
// disjoint volume-annotation lists that live on LedgerLog directly (what
// readLog / applyVolumeAnnotations consume).
func createdTxLog(
	seq uint64,
	ledger string,
	ts *commonpb.Timestamp,
	postings []*commonpb.Posting,
	newKept, purged, ephemeral []*commonpb.TouchedVolume,
) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id:               seq,
						NewKeptVolumes:   newKept,
						PurgedVolumes:    purged,
						EphemeralVolumes: ephemeral,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: &commonpb.Transaction{
										Id:        seq,
										Postings:  postings,
										Timestamp: ts,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// revertedTxLog builds an Apply log carrying a RevertedTransaction plus the
// volume-annotation lists.
func revertedTxLog(
	seq uint64,
	ledger string,
	ts *commonpb.Timestamp,
	postings []*commonpb.Posting,
	newKept, purged, ephemeral []*commonpb.TouchedVolume,
) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id:               seq,
						NewKeptVolumes:   newKept,
						PurgedVolumes:    purged,
						EphemeralVolumes: ephemeral,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
								RevertedTransaction: &commonpb.RevertedTransaction{
									RevertTransaction: &commonpb.Transaction{
										Id:        seq,
										Postings:  postings,
										Timestamp: ts,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// createTxOrder wraps a CreateTransactionOrder in an Order the way the FSM
// serializes it into an AuditItem (raftcmdpb.Order → SerializedOrder).
func createTxOrder(ledger string, order *raftcmdpb.CreateTransactionOrder) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: order},
					},
				},
			},
		},
	}
}

// revertTxOrder wraps a RevertTransactionOrder in an Order.
func revertTxOrder(ledger string, order *raftcmdpb.RevertTransactionOrder) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{RevertTransaction: order},
					},
				},
			},
		},
	}
}

// mirrorCreatedOrder wraps a MirrorIngest carrying a created-transaction entry.
func mirrorCreatedOrder(ledger, reference string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Entry: &raftcmdpb.MirrorLogEntry{
							Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
								CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{Reference: reference},
							},
						},
					},
				},
			},
		},
	}
}

// seedAuditItem is a single order in a synthetic audit entry: the order to
// serialize, the log sequence it produced (0 → skipped/non-log-producing), and
// the log to persist at that sequence (nil → no log row).
type seedAuditItem struct {
	order  *raftcmdpb.Order
	logSeq uint64
	log    *commonpb.Log
}

// seedAuditEntry describes one synthetic audit entry to write into the primary
// store. success=false stages a failed proposal (GetSuccess() == nil, no
// AppliedProposal). transientVolumes stages an AppliedProposal.TransientVolumes
// map keyed by ledger.
type seedAuditEntry struct {
	seq              uint64
	success          bool
	items            []seedAuditItem
	transientVolumes map[string][]*commonpb.TouchedVolume
}

// seedAuditData writes the full fixture (audit entries, items, applied
// proposals and logs) into the primary store in a single committed batch.
func seedAuditData(t *testing.T, store *dal.Store, entries []seedAuditEntry) {
	t.Helper()

	batch := store.OpenWriteSession()

	for _, e := range entries {
		auditEntry := &auditpb.AuditEntry{Sequence: e.seq}
		if e.success {
			auditEntry.Outcome = &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}
		} else {
			auditEntry.Outcome = &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}}
		}

		require.NoError(t, batch.SetProto(usageColdAuditKey(e.seq), auditEntry))

		// Only success entries carry an AppliedProposal — a failed proposal
		// leaves a gap (ReadAppliedProposal → ErrNotFound), which is exactly
		// the branch processAuditEntries skips via GetSuccess() == nil.
		if e.success {
			proposal := &proposalpb.AppliedProposal{Sequence: e.seq}
			if len(e.transientVolumes) > 0 {
				proposal.TransientVolumes = make(map[string]*proposalpb.TouchedVolumeList, len(e.transientVolumes))
				for ledger, vols := range e.transientVolumes {
					proposal.TransientVolumes[ledger] = &proposalpb.TouchedVolumeList{Volumes: vols}
				}
			}

			require.NoError(t, batch.SetProto(usageColdAppliedProposalKey(e.seq), proposal))
		}

		for idx, item := range e.items {
			raw, err := item.order.MarshalVT()
			require.NoError(t, err)

			auditItem := &auditpb.AuditItem{
				OrderIndex:      uint32(idx),
				SerializedOrder: raw,
				LogSequence:     item.logSeq,
			}
			require.NoError(t, batch.SetProto(usageColdAuditItemKey(e.seq, uint32(idx)), auditItem))

			if item.log != nil {
				require.NoError(t, batch.SetProto(usageColdLogKey(item.logSeq), item.log))
			}
		}
	}

	require.NoError(t, batch.Commit())
}

// newTestBuilder constructs a Builder wired to the two stores with the given
// batch size (uses a no-op logger).
func newTestBuilder(primary *dal.Store, usage *usagestore.Store, batchSize int) *Builder {
	return &Builder{
		pebbleStore: primary,
		usageStore:  usage,
		logger:      logging.NopZap(),
		batchSize:   batchSize,
	}
}

// ---------------------------------------------------------------------------
// Table-driven end-to-end tests for the ingestion pipeline.
// ---------------------------------------------------------------------------

func TestProcessAuditEntries_Dispatch(t *testing.T) {
	t.Parallel()

	const ledger = "l1"

	ts := &commonpb.Timestamp{Data: 1234}

	type wantCounter struct {
		id    byte
		value uint64
	}

	type wantTemplate struct {
		name     string
		count    uint64
		lastUsed uint64
	}

	tests := []struct {
		name          string
		entries       []seedAuditEntry
		wantCursor    uint64
		wantCounters  []wantCounter
		zeroCounters  []byte // counters expected to be absent/zero
		wantTemplates []wantTemplate
	}{
		{
			// Case 1: single scripted CreateTransaction with a reference and one
			// new-kept volume — posting/reference/numscript/volume/template all fire.
			name: "single_create_transaction_scripted_with_reference",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				items: []seedAuditItem{{
					order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
						Reference:          "ref-1",
						Timestamp:          ts,
						NumscriptReference: &raftcmdpb.NumscriptReference{Name: "payout"},
					}),
					logSeq: 10,
					log: createdTxLog(10, ledger, ts,
						[]*commonpb.Posting{usagePosting("world", "alice", "USD", 100)},
						[]*commonpb.TouchedVolume{touchedVolume("alice", "USD", "")},
						nil, nil),
				}},
			}},
			wantCursor: 1,
			wantCounters: []wantCounter{
				{usagestore.CounterPosting, 1},
				{usagestore.CounterReference, 1},
				{usagestore.CounterNumscriptExecution, 1},
				{usagestore.CounterVolume, 1},
			},
			zeroCounters:  []byte{usagestore.CounterRevert, usagestore.CounterEphemeralEvicted},
			wantTemplates: []wantTemplate{{"payout", 1, 1234}},
		},
		{
			// Case 1b: a non-scripted create with no reference contributes only
			// posting + volume; no reference / numscript / template.
			name: "single_create_transaction_plain_no_reference",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				items: []seedAuditItem{{
					order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
						Postings: []*commonpb.Posting{usagePosting("world", "bob", "USD", 5)},
					}),
					logSeq: 10,
					log: createdTxLog(10, ledger, nil,
						[]*commonpb.Posting{usagePosting("world", "bob", "USD", 5)},
						[]*commonpb.TouchedVolume{touchedVolume("bob", "USD", "")},
						nil, nil),
				}},
			}},
			wantCursor: 1,
			wantCounters: []wantCounter{
				{usagestore.CounterPosting, 1},
				{usagestore.CounterVolume, 1},
			},
			zeroCounters: []byte{usagestore.CounterReference, usagestore.CounterNumscriptExecution, usagestore.CounterRevert},
		},
		{
			// Case 2: RevertTransaction — revert counter + posting + the
			// reversal's purged volume (volume -1, ephemeral-evicted +1).
			name: "revert_transaction",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				items: []seedAuditItem{{
					order:  revertTxOrder(ledger, &raftcmdpb.RevertTransactionOrder{TransactionId: 7}),
					logSeq: 10,
					log: revertedTxLog(10, ledger, ts,
						[]*commonpb.Posting{usagePosting("alice", "world", "USD", 100)},
						nil,
						[]*commonpb.TouchedVolume{touchedVolume("alice", "USD", "")},
						nil),
				}},
			}},
			wantCursor: 1,
			wantCounters: []wantCounter{
				{usagestore.CounterRevert, 1},
				{usagestore.CounterPosting, 1},
				{usagestore.CounterEphemeralEvicted, 1},
			},
			// One purged volume: CounterVolume was decremented from 0 → clamped to 0.
			zeroCounters: []byte{usagestore.CounterVolume, usagestore.CounterReference},
		},
		{
			// Case 3: MirrorIngest created-transaction — posting + reference +
			// volume, but NO numscript/template (client metadata doesn't cross
			// the mirror wire).
			name: "mirror_ingest_created_transaction",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				items: []seedAuditItem{{
					order:  mirrorCreatedOrder(ledger, "mirror-ref"),
					logSeq: 10,
					log: createdTxLog(10, ledger, ts,
						[]*commonpb.Posting{usagePosting("world", "carol", "USD", 3)},
						[]*commonpb.TouchedVolume{touchedVolume("carol", "USD", "")},
						nil, nil),
				}},
			}},
			wantCursor: 1,
			wantCounters: []wantCounter{
				{usagestore.CounterPosting, 1},
				{usagestore.CounterReference, 1},
				{usagestore.CounterVolume, 1},
			},
			zeroCounters: []byte{usagestore.CounterNumscriptExecution, usagestore.CounterRevert},
		},
		{
			// Case 4: multi-order dedup within one audit entry — a shared
			// (account, asset) touched by three orders contributes to
			// CounterVolume exactly once (entryVolumeState dedup).
			name: "multi_order_shared_volume_dedup_within_entry",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				items: []seedAuditItem{
					{
						order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
							Postings: []*commonpb.Posting{usagePosting("world", "bank:main", "USD", 1)},
						}),
						logSeq: 10,
						log: createdTxLog(10, ledger, nil,
							[]*commonpb.Posting{usagePosting("world", "bank:main", "USD", 1)},
							[]*commonpb.TouchedVolume{touchedVolume("bank:main", "USD", "")},
							nil, nil),
					},
					{
						order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
							Postings: []*commonpb.Posting{usagePosting("world", "bank:main", "USD", 2)},
						}),
						logSeq: 11,
						log: createdTxLog(11, ledger, nil,
							[]*commonpb.Posting{usagePosting("world", "bank:main", "USD", 2)},
							[]*commonpb.TouchedVolume{touchedVolume("bank:main", "USD", "")},
							nil, nil),
					},
					{
						order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
							Postings: []*commonpb.Posting{usagePosting("world", "bank:main", "USD", 3)},
						}),
						logSeq: 12,
						log: createdTxLog(12, ledger, nil,
							[]*commonpb.Posting{usagePosting("world", "bank:main", "USD", 3)},
							[]*commonpb.TouchedVolume{touchedVolume("bank:main", "USD", "")},
							nil, nil),
					},
				},
			}},
			wantCursor: 1,
			wantCounters: []wantCounter{
				// Postings are per-event: 3 orders → 3.
				{usagestore.CounterPosting, 3},
				// Volume is per-entry cardinality: shared tuple counts once.
				{usagestore.CounterVolume, 1},
			},
		},
		{
			// Case 5: color dedup within one audit entry (regression guard) —
			// two DISTINCT colors of the same (account, asset) each count
			// independently. Two new-kept color buckets → CounterVolume == 2;
			// two purged color buckets → CounterEphemeralEvicted == 2.
			name: "distinct_colors_same_account_count_independently",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				items: []seedAuditItem{{
					order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
						Postings: []*commonpb.Posting{usagePosting("world", "vault", "USD", 1)},
					}),
					logSeq: 10,
					log: createdTxLog(10, ledger, nil,
						[]*commonpb.Posting{usagePosting("world", "vault", "USD", 1)},
						[]*commonpb.TouchedVolume{
							touchedVolume("vault", "USD", "red"),
							touchedVolume("vault", "USD", "blue"),
						},
						[]*commonpb.TouchedVolume{
							touchedVolume("vault", "EUR", "red"),
							touchedVolume("vault", "EUR", "blue"),
						},
						nil),
				}},
			}},
			wantCursor: 1,
			wantCounters: []wantCounter{
				{usagestore.CounterPosting, 1},
				// 2 new-kept color buckets (+2), 2 purged color buckets (-2) → 0.
				{usagestore.CounterVolume, 0},
				// Both purged color buckets are evictions → 2.
				{usagestore.CounterEphemeralEvicted, 2},
			},
		},
		{
			// Case 6: failed proposal contributes nothing. A success entry
			// alongside it proves processing continues and the cursor still
			// advances past the failure.
			name: "failed_proposal_skipped",
			entries: []seedAuditEntry{
				{seq: 1, success: false},
				{
					seq:     2,
					success: true,
					items: []seedAuditItem{{
						order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
							Postings: []*commonpb.Posting{usagePosting("world", "dave", "USD", 9)},
						}),
						logSeq: 10,
						log: createdTxLog(10, ledger, nil,
							[]*commonpb.Posting{usagePosting("world", "dave", "USD", 9)},
							nil, nil, nil),
					}},
				},
			},
			wantCursor:   2,
			wantCounters: []wantCounter{{usagestore.CounterPosting, 1}},
		},
		{
			// Case 8: LogSequence == 0 item is skipped (no log-producing order).
			// The entry still advances the cursor but contributes no counters.
			name: "log_sequence_zero_item_skipped",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				items: []seedAuditItem{{
					order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
						Postings: []*commonpb.Posting{usagePosting("world", "eve", "USD", 1)},
					}),
					logSeq: 0, // idempotent replay / non-log-producing → skip
					log:    nil,
				}},
			}},
			wantCursor:   1,
			zeroCounters: []byte{usagestore.CounterPosting, usagestore.CounterVolume},
		},
		{
			// TransientVolumes on the AppliedProposal → CounterTransientUsed.
			name: "transient_volumes_from_applied_proposal",
			entries: []seedAuditEntry{{
				seq:     1,
				success: true,
				transientVolumes: map[string][]*commonpb.TouchedVolume{
					ledger: {
						touchedVolume("tmp:1", "USD", ""),
						touchedVolume("tmp:2", "USD", ""),
					},
				},
			}},
			wantCursor:   1,
			wantCounters: []wantCounter{{usagestore.CounterTransientUsed, 2}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			primary, usage := newUsageTestStores(t)
			seedAuditData(t, primary, tc.entries)

			b := newTestBuilder(primary, usage, 200)

			cursor, err := b.processAuditEntries(context.Background(), 0, time.Time{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantCursor, cursor, "cursor must advance to the last audit sequence")
			assert.Equal(t, tc.wantCursor, b.LastProcessedAuditSequence(), "atomic hint must mirror the cursor")

			for _, wc := range tc.wantCounters {
				got, err := usage.GetCounter(ledger, wc.id)
				require.NoError(t, err)
				assert.Equalf(t, wc.value, got, "counter %#x", wc.id)
			}

			for _, id := range tc.zeroCounters {
				got, err := usage.GetCounter(ledger, id)
				require.NoError(t, err)
				assert.Equalf(t, uint64(0), got, "counter %#x must be zero", id)
			}

			for _, wt := range tc.wantTemplates {
				got, err := usage.GetTemplateUsage(ledger, wt.name)
				require.NoError(t, err)
				require.NotNilf(t, got, "template %q must exist", wt.name)
				assert.Equalf(t, wt.count, got.GetCount(), "template %q count", wt.name)
				assert.Equalf(t, wt.lastUsed, got.GetLastUsed().GetData(), "template %q lastUsed", wt.name)
			}

			// The persisted progress cursor must match the returned cursor.
			progress, err := usage.ReadProgress()
			require.NoError(t, err)
			assert.Equal(t, tc.wantCursor, progress, "persisted progress must equal the cursor")
		})
	}
}

// TestProcessAuditEntries_SkippedCreateDoesNotCount guards the isCreatedTx gate:
// a CreateTransaction that produced an OrderSkipped log (no CreatedTransaction /
// RevertedTransaction payload) must contribute no reference / numscript /
// template counters even though the order carried them.
func TestProcessAuditEntries_SkippedCreateDoesNotCount(t *testing.T) {
	t.Parallel()

	const ledger = "l1"

	primary, usage := newUsageTestStores(t)

	// A log with no CreatedTransaction/RevertedTransaction payload models a
	// skipped order: readLog reports isCreatedTx == false.
	skippedLog := &commonpb.Log{
		Sequence: 10,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log:        &commonpb.LedgerLog{Id: 10},
				},
			},
		},
	}

	seedAuditData(t, primary, []seedAuditEntry{{
		seq:     1,
		success: true,
		items: []seedAuditItem{{
			order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
				Reference:          "ref-1",
				NumscriptReference: &raftcmdpb.NumscriptReference{Name: "payout"},
			}),
			logSeq: 10,
			log:    skippedLog,
		}},
	}})

	b := newTestBuilder(primary, usage, 200)

	cursor, err := b.processAuditEntries(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), cursor)

	for _, id := range []byte{
		usagestore.CounterReference,
		usagestore.CounterNumscriptExecution,
		usagestore.CounterPosting,
		usagestore.CounterVolume,
	} {
		got, err := usage.GetCounter(ledger, id)
		require.NoError(t, err)
		assert.Equalf(t, uint64(0), got, "skipped create must not bump counter %#x", id)
	}

	tpl, err := usage.GetTemplateUsage(ledger, "payout")
	require.NoError(t, err)
	assert.Nil(t, tpl, "skipped create must not record template usage")
}

// TestProcessAuditEntries_BatchBoundaryAndCursorAdvance covers case 7: with
// batchSize=2 and three entries, all are processed across batches, the returned
// cursor equals the last sequence, and a second call from that cursor is a
// no-op (EOF).
func TestProcessAuditEntries_BatchBoundaryAndCursorAdvance(t *testing.T) {
	t.Parallel()

	const ledger = "l1"

	primary, usage := newUsageTestStores(t)

	var entries []seedAuditEntry
	for seq := uint64(1); seq <= 3; seq++ {
		logSeq := 10 + seq
		entries = append(entries, seedAuditEntry{
			seq:     seq,
			success: true,
			items: []seedAuditItem{{
				order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
					Postings: []*commonpb.Posting{usagePosting("world", "acc", "USD", seq)},
				}),
				logSeq: logSeq,
				log: createdTxLog(logSeq, ledger, nil,
					[]*commonpb.Posting{usagePosting("world", "acc", "USD", seq)},
					nil, nil, nil),
			}},
		})
	}

	seedAuditData(t, primary, entries)

	b := newTestBuilder(primary, usage, 2) // batchSize=2 → two commits (2 then 1)

	cursor, err := b.processAuditEntries(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(3), cursor, "all three entries processed across batch boundaries")

	got, err := usage.GetCounter(ledger, usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), got, "one posting per entry across both batches")

	// Persisted progress reflects the full drain.
	progress, err := usage.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), progress)

	// A second call from the advanced cursor is a no-op: nothing new to read,
	// clean EOF, cursor unchanged.
	cursor2, err := b.processAuditEntries(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), cursor2, "re-running from the head is an idempotent no-op")

	got, err = usage.GetCounter(ledger, usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), got, "no-op pass must not double-count")
}

// TestProcessAuditEntries_ResumeFromCursorSkipsProcessed verifies the cursor
// filter: starting from a non-zero cursor skips already-consumed entries and
// only folds the tail.
func TestProcessAuditEntries_ResumeFromCursorSkipsProcessed(t *testing.T) {
	t.Parallel()

	const ledger = "l1"

	primary, usage := newUsageTestStores(t)

	var entries []seedAuditEntry
	for seq := uint64(1); seq <= 3; seq++ {
		logSeq := 100 + seq
		entries = append(entries, seedAuditEntry{
			seq:     seq,
			success: true,
			items: []seedAuditItem{{
				order: createTxOrder(ledger, &raftcmdpb.CreateTransactionOrder{
					Postings: []*commonpb.Posting{usagePosting("world", "acc", "USD", seq)},
				}),
				logSeq: logSeq,
				log: createdTxLog(logSeq, ledger, nil,
					[]*commonpb.Posting{usagePosting("world", "acc", "USD", seq)},
					nil, nil, nil),
			}},
		})
	}

	seedAuditData(t, primary, entries)

	b := newTestBuilder(primary, usage, 200)

	// Resume from cursor=2: only entry seq 3 should be folded.
	cursor, err := b.processAuditEntries(context.Background(), 2, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), cursor)

	got, err := usage.GetCounter(ledger, usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), got, "only the single post-cursor entry must be folded")
}
