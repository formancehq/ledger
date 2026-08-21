package grpc

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

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

// TestValidateEventFromCheckEvent pins the classification `restore validate`
// turns into an exit status.
//
// ValidateRestore runs the checker with no cold reader, so on an archived
// cluster the signing pass reports coverage it could not complete on EVERY run,
// healthy or not. Marking those findings makes the CLI count divergences only;
// dropping the mark makes a valid backup of an archived cluster exit non-zero
// and stops `restore validate && restore finalize` on a good backup.
//
// The divergence cases assert the negative just as hard: over-marking would let
// real corruption through as a warning, which is the failure this must not trade
// for.
func TestValidateEventFromCheckEvent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		errorType       servicepb.CheckStoreErrorType
		wantCoverageGap bool
	}{
		{
			name:            "signing coverage gap",
			errorType:       servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			wantCoverageGap: true,
		},
		{
			name:            "log coverage gap",
			errorType:       servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE,
			wantCoverageGap: true,
		},
		{
			// The widest gap of the three: it covers the projection comparisons
			// wholesale, and it is the one an archived backup always reports.
			name:            "archived state coverage gap",
			errorType:       servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ARCHIVED_STATE_VERIFICATION_INCOMPLETE,
			wantCoverageGap: true,
		},
		{
			name:            "hash mismatch is a divergence",
			errorType:       servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
			wantCoverageGap: false,
		},
		{
			name:            "unaudited log is a divergence",
			errorType:       servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED,
			wantCoverageGap: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := validateEventFromCheckEvent(&servicepb.CheckStoreEvent{
				Type: &servicepb.CheckStoreEvent_Error{
					Error: &servicepb.CheckStoreError{
						ErrorType: tc.errorType,
						Message:   "finding text",
					},
				},
			})

			require.Equal(t, "finding text", got.GetError().GetMessage(),
				"the operator must still see the finding either way")
			require.Equal(t, tc.wantCoverageGap, got.GetError().GetCoverageGap(),
				"%s decides whether `restore validate` exits non-zero", tc.errorType)
		})
	}
}

// TestValidateEventFromCheckEventCarriesProgress keeps the progress arm from
// being lost when the error arm is edited: the CLI drives its spinner off these,
// and a dropped arm would send an empty event on every progress tick.
func TestValidateEventFromCheckEventCarriesProgress(t *testing.T) {
	t.Parallel()

	got := validateEventFromCheckEvent(&servicepb.CheckStoreEvent{
		Type: &servicepb.CheckStoreEvent_Progress{
			Progress: &servicepb.CheckStoreProgress{LogsChecked: 7, TotalLogs: 11},
		},
	})

	require.Equal(t, uint64(7), got.GetProgress().GetLogsChecked())
	require.Equal(t, uint64(11), got.GetProgress().GetTotalLogs())
	require.Nil(t, got.GetError())
}
