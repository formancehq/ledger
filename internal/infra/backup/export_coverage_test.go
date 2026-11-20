package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// applyCoverageTransfer applies one forced transfer between two accounts that
// no earlier proposal touched, so zero-value volume preloads are exact.
func applyCoverageTransfer(t *testing.T, machine *state.Machine, store *dal.Store, index uint64, ledger, source, destination string, amount uint64) {
	t.Helper()

	posting := &commonpb.Posting{Source: source, Destination: destination, Asset: "USD", Amount: commonpb.NewUint256FromUint64(amount)}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger,
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Postings: []*commonpb.Posting{posting}, Force: true,
			}},
		}},
	}}}

	ledgerID, ledgerTag := attributes.MakeKey(domain.LedgerKey{Name: ledger}.Bytes())
	plan := &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{
		{Id: &raftcmdpb.AttributeID{Id: ledgerID[:], Tag: ledgerTag}, AttrCode: uint32(dal.SubAttrLedger)},
		{Id: &raftcmdpb.AttributeID{Id: ledgerID[:], Tag: ledgerTag}, AttrCode: uint32(dal.SubAttrBoundary)},
	}}
	zero, err := (&raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)}).MarshalVT()
	require.NoError(t, err)
	for _, account := range []string{source, destination} {
		id, tag := attributes.MakeKey(domain.NewVolumeKey(ledger, account, "USD", "").Bytes())
		plan.Attributes = append(plan.Attributes, &raftcmdpb.AttributeCoverage{
			Id: &raftcmdpb.AttributeID{Id: id[:], Tag: tag}, AttrCode: uint32(dal.SubAttrVolume),
			Value: &raftcmdpb.AttributeValue{RawValue: zero},
		})
	}
	bits := make([]byte, (len(plan.GetAttributes())+7)/8)
	for i := range plan.GetAttributes() {
		bits[i/8] |= 1 << (i % 8)
	}
	order.Technical = &raftcmdpb.OrderTechnical{CoverageBits: bits}

	proposal := &raftcmdpb.Proposal{Id: index, Orders: []*raftcmdpb.Order{order}, Date: &commonpb.Timestamp{Data: 1700000000 + index},
		ExecutionPlan: plan, CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterConfig)}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	result, err := machine.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
	require.NoError(t, err)
	for _, r := range result.Results {
		require.NoError(t, r.Error)
	}
}

func writeAppliedProposalCoverageRow(t *testing.T, store *dal.Store, seq uint64, success bool) {
	t.Helper()

	batch := store.OpenWriteSession()
	audit := &auditpb.AuditEntry{Sequence: seq}
	if success {
		audit.Outcome = &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}
	} else {
		audit.Outcome = &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}}
	}
	auditKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(seq).Build()
	if err := batch.SetProto(auditKey, audit); err != nil {
		t.Fatal(err)
	}
	if success {
		proposalKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAppliedProposal).PutUint64(seq).Build()
		if err := batch.SetProto(proposalKey, &proposalpb.AppliedProposal{Sequence: seq}); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}
}

func deleteExportedSegment(t *testing.T, manifest *Manifest, typ string) *Manifest {
	t.Helper()
	clonedManifest := *manifest
	clonedManifest.Exports = slices.DeleteFunc(slices.Clone(manifest.Exports), func(segment ExportSegment) bool { return segment.Type == typ })

	return &clonedManifest
}

func TestValidateAppliedProposalCoverage_AllowsFailureGaps(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	writeAppliedProposalCoverageRow(t, store, 1, true)
	writeAppliedProposalCoverageRow(t, store, 2, false)
	writeAppliedProposalCoverageRow(t, store, 3, true)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	require.NoError(t, validateAppliedProposalCoverage(t.Context(), handle, 0))
}

func TestValidateAppliedProposalCoverageRejectsMissingSuccessCompanion(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	writeAppliedProposalCoverageRow(t, store, 1, true)
	writeAppliedProposalCoverageRow(t, store, 2, false)
	// Sequence 3 is a successful audit with its AppliedProposal segment/row
	// omitted, which is the EN-3308 restore shape.
	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(3).Build()
	require.NoError(t, batch.SetProto(key, &auditpb.AuditEntry{Sequence: 3, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 3, MaxLogSequence: 3}}}))
	require.NoError(t, batch.Commit())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	err = validateAppliedProposalCoverage(t.Context(), handle, 0)
	require.ErrorContains(t, err, "successful audit sequence 3 is missing its applied proposal")
}

func TestValidateAppliedProposalCoverageRejectsOrphan(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	writeAppliedProposalCoverageRow(t, store, 1, false)
	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAppliedProposal).PutUint64(2).Build()
	require.NoError(t, batch.SetProto(key, &proposalpb.AppliedProposal{Sequence: 2}))
	require.NoError(t, batch.Commit())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	err = validateAppliedProposalCoverage(t.Context(), handle, 0)
	require.ErrorContains(t, err, "applied proposal sequence 2 has no audit entry")
}

func coverageVolume(t *testing.T, store *dal.Store, attrs *attributes.Attributes, ledger, account string) *raftcmdpb.VolumePair {
	t.Helper()

	pair, err := attrs.Volume.Get(store, domain.NewVolumeKey(ledger, account, "USD", "").Bytes())
	require.NoError(t, err)

	return pair
}

// TestBackup_RejectsManifestMissingInteriorLogSegment restores a real
// checkpoint plus three single-transfer incremental deltas. Every remaining
// export object stays individually valid when the middle log object is removed
// from the manifest, so only manifest-wide coverage can reject the restore
// before offline bootstrap activates a store missing that transfer's postings.
func TestBackup_RejectsManifestMissingInteriorLogSegment(t *testing.T) {
	t.Parallel()

	const (
		bucketID = "bucket"
		ledger   = "ledger"
	)

	src, store, attrs := newSigningParityMachine(t)
	require.NoError(t, applyMetadataCreation(t, src, store, 1, metadataCreationOrder(ledger, commonpb.LedgerMode_LEDGER_MODE_NORMAL, nil))[0].Error)

	storage := newInMemoryBackupStorage()
	full, err := RunBackup(t.Context(), testLogger(), store, storage, bucketID, "full")
	require.NoError(t, err)
	require.Positive(t, full.LastLogSequence)
	intactDir, gappedDir := t.TempDir(), t.TempDir()
	require.NoError(t, store.Checkpoint(filepath.Join(intactDir, "live")))
	require.NoError(t, store.Checkpoint(filepath.Join(gappedDir, "live")))
	tailDirs := map[string]string{}
	for _, typ := range []string{"log", "audit", "auditItem"} {
		tailDirs[typ] = t.TempDir()
		require.NoError(t, store.Checkpoint(filepath.Join(tailDirs[typ], "live")))
	}

	transfers := []struct {
		source, destination string
		amount              uint64
	}{{"src:1", "dst:1", 10}, {"src:2", "dst:2", 20}, {"src:3", "dst:3", 30}}
	for i, tr := range transfers {
		applyCoverageTransfer(t, src, store, uint64(i+2), ledger, tr.source, tr.destination, tr.amount)
		inc, err := RunIncrementalBackup(t.Context(), testLogger(), store, storage, bucketID, 0)
		require.NoError(t, err)
		require.Positive(t, inc.LogEntriesExported)
	}

	manifest, err := ReadManifest(t.Context(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	logSegments := slices.DeleteFunc(slices.Clone(manifest.Exports), func(seg ExportSegment) bool { return seg.Type != "log" })
	require.Len(t, logSegments, len(transfers), "each transfer must be exported in its own log segment")
	require.Equal(t, full.LastLogSequence+1, logSegments[0].StartSeq)

	openRestored := func(dir string) *dal.Store {
		restored, err := dal.NewStore(dir, testLogger(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, restored.Close()) })

		return restored
	}

	intact := openRestored(intactDir)
	require.NoError(t, ApplyExportsAndRebuild(context.Background(), testLogger(), storage, intact, manifest))
	intactAttrs := attributes.New()
	for _, tr := range transfers {
		for _, account := range []string{tr.source, tr.destination} {
			want := coverageVolume(t, store, attrs, ledger, account)
			require.True(t, want.EqualVT(coverageVolume(t, intact, intactAttrs, ledger, account)), "account %s", account)
		}
	}
	require.Empty(t, creationIntegrityFindings(t, intact, intactAttrs))
	require.NotNil(t, manifest.ExportBoundary)
	for _, typ := range []string{"log", "audit", "auditItem"} {
		t.Run("missing final "+typ, func(t *testing.T) {
			t.Parallel()
			dir := tailDirs[typ]
			truncated := *manifest
			truncated.Exports = slices.Clone(manifest.Exports)
			for i, v := range slices.Backward(truncated.Exports) {
				if v.Type == typ {
					truncated.Exports = slices.Delete(truncated.Exports, i, i+1)

					break
				}
			}
			restored, err := dal.NewStore(dir, testLogger(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
			require.NoError(t, err)
			defer func() { require.NoError(t, restored.Close()) }()
			err = ApplyExportsAndRebuild(t.Context(), testLogger(), storage, restored, &truncated)
			require.ErrorIs(t, err, ErrExportCoverageGap)
			require.ErrorContains(t, err, `"`+typ+`"`)
			_, markerErr := os.Stat(filepath.Join(dir, "RESTORED"))
			require.ErrorIs(t, markerErr, os.ErrNotExist)
		})
	}

	middle := logSegments[1]
	gapped := *manifest
	gapped.Exports = slices.DeleteFunc(slices.Clone(manifest.Exports), func(seg ExportSegment) bool { return seg.Key == middle.Key })
	require.Len(t, gapped.Exports, len(manifest.Exports)-1)

	gappedStore := openRestored(gappedDir)
	err = ApplyExportsAndRebuild(context.Background(), testLogger(), storage, gappedStore, &gapped)
	require.ErrorIs(t, err, ErrExportCoverageGap)
	require.ErrorContains(t, err, `"log"`)

	handle, err := gappedStore.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()
	lastLog, err := query.ReadLastLog(handle)
	require.NoError(t, err)
	require.Equal(t, full.LastLogSequence, lastLog.GetSequence(), "no export may be applied before the coverage check")
}

func TestBackup_RejectsManifestMissingOnlyAppliedProposalSegment(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"
	src, store, _ := newSigningParityMachine(t)
	require.NoError(t, applyMetadataCreation(t, src, store, 1, metadataCreationOrder("before", commonpb.LedgerMode_LEDGER_MODE_NORMAL, nil))[0].Error)
	storage := newInMemoryBackupStorage()
	full, err := RunBackup(t.Context(), testLogger(), store, storage, bucketID, "full")
	require.NoError(t, err)
	dstDir := t.TempDir()
	require.NoError(t, store.Checkpoint(filepath.Join(dstDir, "live")))
	require.NoError(t, applyMetadataCreation(t, src, store, 2, metadataCreationOrder("after", commonpb.LedgerMode_LEDGER_MODE_NORMAL, nil))[0].Error)
	_, err = RunIncrementalBackup(t.Context(), testLogger(), store, storage, bucketID, 0)
	require.NoError(t, err)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.NotNil(t, findExport(manifest, "appliedProposal"))
	require.Greater(t, manifest.LastExportAuditSequence(), full.LastAuditSequence)

	withoutProposalSegment := deleteExportedSegment(t, manifest, "appliedProposal")
	dst, err := dal.NewStore(dstDir, testLogger(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dst.Close()) })
	err = ApplyExportsAndRebuild(t.Context(), testLogger(), storage, dst, withoutProposalSegment)
	require.ErrorContains(t, err, "successful audit sequence")
	require.ErrorContains(t, err, "missing its applied proposal")
}

func TestValidateExportCoverage(t *testing.T) {
	t.Parallel()

	seg := func(typ string, start, end uint64) ExportSegment {
		return ExportSegment{Type: typ, StartSeq: start, EndSeq: end, Key: fmt.Sprintf("%s-%d-%d", typ, start, end)}
	}
	checkpoint := &CheckpointManifest{LastLogSequence: 10, LastAuditSequence: 5}

	for _, tc := range []struct {
		name       string
		checkpoint *CheckpointManifest
		exports    []ExportSegment
		wantErr    string
	}{
		{name: "contiguous rounds", checkpoint: checkpoint, exports: []ExportSegment{
			seg("log", 11, 12), seg("audit", 6, 7), seg("auditItem", 6, 7), seg("appliedProposal", 6, 6),
			seg("log", 13, 13), seg("audit", 8, 8), seg("auditItem", 8, 8),
			seg("log", 14, 20), seg("audit", 9, 12), seg("auditItem", 9, 10), seg("auditItem", 11, 12), seg("appliedProposal", 10, 12),
		}},
		{name: "export-only from sequence one", exports: []ExportSegment{
			seg("log", 1, 3), seg("audit", 1, 2), seg("auditItem", 1, 2),
		}},
		{name: "first log after boundary missing", checkpoint: checkpoint, exports: []ExportSegment{seg("log", 12, 12)},
			wantErr: `"log" segment log-12-12 starts at 12, expected 11`},
		{name: "interior log segment missing", checkpoint: checkpoint, exports: []ExportSegment{seg("log", 11, 11), seg("log", 13, 13)},
			wantErr: `"log" segment log-13-13 starts at 13, expected 12`},
		{name: "overlapping log segments", checkpoint: checkpoint, exports: []ExportSegment{seg("log", 11, 12), seg("log", 12, 13)},
			wantErr: `"log" segment log-12-13 starts at 12, expected 13`},
		{name: "reordered audit segments", checkpoint: checkpoint, exports: []ExportSegment{
			seg("audit", 7, 7), seg("audit", 6, 6), seg("auditItem", 6, 7),
		}, wantErr: `"audit" segment audit-7-7 starts at 7, expected 6`},
		{name: "log re-exports checkpoint history", checkpoint: checkpoint, exports: []ExportSegment{seg("log", 10, 11)},
			wantErr: `"log" segment log-10-11 starts at 10, expected 11`},
		{name: "export-only starts at zero", exports: []ExportSegment{seg("log", 0, 1)},
			wantErr: `"log" segment log-0-1 starts at 0, expected 1`},
		{name: "interior audit item segment missing", checkpoint: checkpoint, exports: []ExportSegment{
			seg("audit", 6, 8), seg("auditItem", 6, 6), seg("auditItem", 8, 8),
		}, wantErr: `"auditItem" segment auditItem-8-8 starts at 8, expected 7`},
		{name: "applied proposals overlap", checkpoint: checkpoint, exports: []ExportSegment{
			seg("audit", 6, 9), seg("auditItem", 6, 9), seg("appliedProposal", 6, 8), seg("appliedProposal", 8, 9),
		}, wantErr: `"appliedProposal" segment appliedProposal-8-9 starts at 8, at or below the covered sequence 8`},
		{name: "applied proposal inside checkpoint", checkpoint: checkpoint, exports: []ExportSegment{
			seg("audit", 6, 6), seg("auditItem", 6, 6), seg("appliedProposal", 5, 6),
		}, wantErr: `"appliedProposal" segment appliedProposal-5-6 starts at 5, at or below the covered sequence 5`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateExportCoverage(&Manifest{Checkpoint: tc.checkpoint, Exports: tc.exports})
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, ErrExportCoverageGap)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	err := validateExportCoverage(&Manifest{Exports: []ExportSegment{seg("bogus", 1, 1)}})
	require.ErrorContains(t, err, `unsupported export segment type "bogus"`)
	err = validateExportCoverage(&Manifest{Checkpoint: checkpoint, ExportBoundary: &ExportBoundary{
		LastLogSequence: 11, LastAuditSequence: 5, LastAuditItemSequence: 5,
	}})
	require.ErrorIs(t, err, ErrExportCoverageGap)
	require.ErrorContains(t, err, `"log"`)

	for _, tc := range []struct {
		name, missing string
	}{
		{name: "log", missing: "log"},
		{name: "audit", missing: "audit"},
		{name: "audit item", missing: "auditItem"},
	} {
		t.Run("snapshot boundary rejects missing final "+tc.name, func(t *testing.T) {
			t.Parallel()
			manifest := &Manifest{Checkpoint: checkpoint, ExportBoundary: &ExportBoundary{LastLogSequence: 12, LastAuditSequence: 7, LastAuditItemSequence: 7},
				Exports: []ExportSegment{seg("log", 11, 12), seg("audit", 6, 7), seg("auditItem", 6, 7)}}
			manifest.Exports = slices.DeleteFunc(manifest.Exports, func(seg ExportSegment) bool { return seg.Type == tc.missing })
			err := validateExportCoverage(manifest)
			require.ErrorIs(t, err, ErrExportCoverageGap)
			require.ErrorContains(t, err, `"`+tc.missing+`"`)
		})
	}
}
