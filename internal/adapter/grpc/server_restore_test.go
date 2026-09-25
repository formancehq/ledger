package grpc

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestFinalizeRestoreRequiresSuccessfulValidation(t *testing.T) {
	t.Parallel()

	server := NewRestoreServiceServer(t.TempDir(), "test-cluster", 1, noopLogger{})
	store, err := dal.OpenDirect(server.stagingDir(), noopLogger{})
	require.NoError(t, err)
	server.mu.Lock()
	server.stagingStore = store
	server.downloaded = true
	server.mu.Unlock()
	t.Cleanup(server.Shutdown)

	_, err = server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "has not passed validation")
}

func TestInvalidCallerAttributionPreventsRestoreFinalization(t *testing.T) {
	t.Parallel()

	const clusterID = "source-cluster"
	server := NewRestoreServiceServer(t.TempDir(), "target-cluster", 1, noopLogger{})
	store, err := dal.OpenDirect(server.stagingDir(), noopLogger{})
	require.NoError(t, err)
	server.mu.Lock()
	server.stagingStore = store
	server.downloaded = true
	server.mu.Unlock()
	t.Cleanup(server.Shutdown)

	configBytes, err := proto.Marshal(&commonpb.PersistedConfig{ClusterId: clusterID})
	require.NoError(t, err)
	entry := &auditpb.AuditEntry{
		Sequence:       1,
		Timestamp:      &commonpb.Timestamp{Data: 1},
		ProposalId:     1,
		HashVersion:    uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		CallerSnapshot: &commonpb.CallerSnapshot{},
		Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{
			Reason:  commonpb.ErrorReason_ERROR_REASON_VALIDATION,
			Message: "rejected",
		}},
	}
	header, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(t, err)
	_, entry.Hash = processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID).
		Compute(nil, nil, [][]byte{header})

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}, configBytes))
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{Sequence: 1}}))
	batch.KeyBuilder.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(entry.GetSequence())
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), entry))
	require.NoError(t, batch.Commit())

	stream := NewMockServerStreamingServer[restorepb.ValidateRestoreEvent](gomock.NewController(t))
	stream.EXPECT().Context().Return(context.Background()).AnyTimes()
	stream.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	require.NoError(t, server.ValidateRestore(&restorepb.ValidateRestoreRequest{}, stream))

	server.mu.Lock()
	validated := server.validated
	server.mu.Unlock()
	require.False(t, validated)
	_, err = server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestSafeStagingPath_Valid(t *testing.T) {
	t.Parallel()

	staging := filepath.Join(t.TempDir(), "staging")

	cases := []struct {
		name string
		in   string
	}{
		{"plain filename", "000123.sst"},
		{"nested forward-slash path", "subdir/000123.sst"},
		{"deep nesting", "a/b/c/d/file.sst"},
		{"dot-prefixed file", ".hidden"},
		{"double dot in middle (not traversal)", "a..b/c.sst"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := safeStagingPath(staging, tc.in)
			require.NoError(t, err)

			rel, err := filepath.Rel(staging, got)
			require.NoError(t, err)
			require.False(t, strings.HasPrefix(rel, ".."),
				"resolved path %q must stay under %q (rel=%q)", got, staging, rel)
		})
	}
}

func TestSafeStagingPath_Rejected(t *testing.T) {
	t.Parallel()

	staging := filepath.Join(t.TempDir(), "staging")

	cases := []struct {
		name       string
		in         string
		wantSubstr string
	}{
		{"empty", "", "empty filename"},
		{"single dot-dot", "..", "escapes"},
		{"parent traversal", "../etc/passwd", "escapes"},
		{"deep traversal", "a/../../etc/passwd", "escapes"},
		{"unix absolute path", "/etc/passwd", "absolute path"},
		{"absolute via leading slash + traversal", "/../etc/passwd", "absolute path"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := safeStagingPath(staging, tc.in)
			require.Error(t, err, "expected rejection for %q", tc.in)
			require.Contains(t, err.Error(), tc.wantSubstr,
				"error %q should mention %q", err.Error(), tc.wantSubstr)
		})
	}
}

// TestSafeStagingPath_DefenseInDepthRel ensures the filepath.Rel guard
// would still reject even if the prefix check missed an edge case. We
// can't easily construct an input that bypasses the prefix check but
// fails Rel, so this test asserts the contract: every accepted name
// has a Rel that stays under the staging root.
func TestSafeStagingPath_DefenseInDepthRel(t *testing.T) {
	t.Parallel()

	staging := filepath.Join(t.TempDir(), "staging")

	// A normal nested file.
	dest, err := safeStagingPath(staging, "checkpoints/0/000123.sst")
	require.NoError(t, err)

	rel, err := filepath.Rel(staging, dest)
	require.NoError(t, err)
	require.Equal(t, filepath.Join("checkpoints", "0", "000123.sst"), rel)
}
