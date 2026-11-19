package readstore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// An audit-only failure at sequence 5 leaves no AppliedProposal; the log fold
// is only at 8 although the full contains log 9. Capture and restore may run
// while the normal and audit projections have reached different points.
func seedTestPrimary(t *testing.T) *dal.Store {
	t.Helper()
	s, err := dal.OpenDirect(t.TempDir(), logging.NopZap())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	batch := s.OpenWriteSession()
	kb := dal.NewKeyBuilder()
	for seq := uint64(8); seq <= 9; seq++ {
		require.NoError(t, batch.SetProto(kb.Reset().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(seq).Build(), &commonpb.Log{Sequence: seq}))
	}
	for seq := uint64(4); seq <= 6; seq++ {
		require.NoError(t, batch.SetProto(kb.Reset().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(seq).Build(), &auditpb.AuditEntry{Sequence: seq}))
	}
	for _, p := range []*proposalpb.AppliedProposal{{Sequence: 4, MinLogSequence: 8, MaxLogSequence: 8}, {Sequence: 6, MinLogSequence: 9, MaxLogSequence: 9}} {
		require.NoError(t, batch.SetProto(kb.Reset().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAppliedProposal).PutUint64(p.GetSequence()).Build(), p))
	}
	require.NoError(t, batch.Commit())

	return s
}

func seedTestCheckpoint(t *testing.T, logSeq, auditSeq, proposalSeq uint64) string {
	t.Helper()
	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, s.WriteProgress(batch, logSeq))
	require.NoError(t, s.WriteAuditProgress(batch, auditSeq))
	require.NoError(t, s.WriteAppliedProposalProgress(batch, proposalSeq))
	require.NoError(t, s.WriteRaftProgress(batch, 42))
	require.NoError(t, s.WriteAuditRaftProgress(batch, 40))
	require.NoError(t, s.WriteBackfillCursor(batch, []byte("task"), []byte("pos")))
	require.NoError(t, batch.Commit())
	dir := filepath.Join(t.TempDir(), "readindex")
	require.NoError(t, s.CreateCheckpoint(dir))

	return dir
}

func TestRestoreSeedKeepsNativeCursorsButClearsSourceCertificates(t *testing.T) {
	t.Parallel()
	primary := seedTestPrimary(t)
	dir := seedTestCheckpoint(t, 8, 5, 4)
	logger := logging.NopZap()
	require.NoError(t, ValidateBackupSeed(dir, logger, primary, 9, 6))

	// A retry can see an already prepared private checkpoint after a crash.
	for range 2 {
		require.NoError(t, PrepareRestoreSeed(dir, logger, primary, 9, 6))
	}
	s, err := OpenReadOnly(dir, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	for _, tc := range []struct {
		key  Uint64Cursor
		want uint64
	}{{progressCursor, 8}, {auditCursor, 5}, {appliedProposalCursor, 4}, {readRaftCursor, 0}, {auditRaftCursor, 0}} {
		got, err := tc.key.Read(s.db)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "key %x", tc.key.key)
	}
	position, found := s.ReadBackfillCursor([]byte("task"))
	require.True(t, found)
	require.Equal(t, []byte("pos"), position)
}

func TestRestoreSeedRejectsUncoveredCursorsBeforeChangingCertificates(t *testing.T) {
	t.Parallel()
	primary := seedTestPrimary(t)
	for _, tc := range []struct {
		name                 string
		log, audit, proposal uint64
		wantErr              string
	}{
		{"log ahead", 10, 5, 4, "log cursor 10 exceeds full checkpoint head 9"},
		{"audit ahead", 8, 7, 4, "audit cursor 7 exceeds full checkpoint head 6"},
		{"proposal ahead", 8, 5, 7, "applied proposal cursor 7 exceeds full checkpoint head 6"},
		{"proposal gap", 8, 5, 5, "applied proposal cursor 5 has no matching record"},
		{"skips next log", 8, 5, 6, "applied proposal 6 ends at log 9 beyond folded log cursor 8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := seedTestCheckpoint(t, tc.log, tc.audit, tc.proposal)
			require.ErrorContains(t, PrepareRestoreSeed(dir, logging.NopZap(), primary, 9, 6), tc.wantErr)
			s, err := OpenReadOnly(dir, logging.NopZap())
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Close()) }()
			got, err := s.ReadRaftProgress()
			require.NoError(t, err)
			require.Equal(t, uint64(42), got)
		})
	}
}

func TestRestoreSeedFromAuditDisabledReplicaKeepsNormalProjection(t *testing.T) {
	t.Parallel()
	primary := seedTestPrimary(t)
	dir := seedTestCheckpoint(t, 8, 0, 4)
	require.NoError(t, PrepareRestoreSeed(dir, logging.NopZap(), primary, 9, 6))

	s, err := OpenReadOnly(dir, logging.NopZap())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	logSeq, err := s.ReadProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(8), logSeq)
	auditSeq, err := s.ReadAuditProgress()
	require.NoError(t, err)
	require.Zero(t, auditSeq, "an enabled destination's audit indexer must rebuild the missing audit projection")
}
