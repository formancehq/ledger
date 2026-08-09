package balancehistorystore

import (
	"context"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	domainhistory "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

func validVerificationManifestAndRun() (Manifest, RunRef) {
	run := RunRef{
		ID:                 1,
		FirstAuditSequence: 1,
		LastAuditSequence:  2,
		MaxLogSequence:     2,
		EntryCount:         1,
		IdentityCount:      1,
		Checksum:           [32]byte{1},
	}
	manifest := Manifest{
		Version:        1,
		AuditWatermark: 2,
		LogWatermark:   2,
		NextRunID:      2,
		Runs:           []RunRef{run},
	}

	return manifest, run
}

func validArchivePart(runID, records uint64, lower, upper []byte) ArchivePart {
	return ArchivePart{
		Ref: balancehistoryarchive.Ref{
			Version:     balancehistoryarchive.FormatVersion,
			SHA256:      [32]byte{1},
			Size:        balancehistoryarchive.EmptyEncodedSize + 1,
			RecordCount: records,
		},
		LowerBound: lower,
		UpperBound: upper,
	}
}

func TestFlattenVerificationTargetsAndSaturatingAdd(t *testing.T) {
	t.Parallel()

	hot := RunRef{ID: 1}
	cold := RunRef{
		ID:           2,
		Archived:     true,
		LocalRemoved: true,
		ArchiveParts: []ArchivePart{{}, {}},
	}
	hybrid := RunRef{ID: 3, Archived: true, ArchiveParts: []ArchivePart{{}}}
	targets := flattenVerificationTargets([]RunRef{hot, cold, hybrid})
	require.Len(t, targets, 5)
	require.True(t, targets[0].hot)
	require.Equal(t, 0, targets[1].partIndex)
	require.Equal(t, 1, targets[2].partIndex)
	require.True(t, targets[3].hot)
	require.False(t, targets[4].hot)

	require.Equal(t, uint64(7), saturatingAdd(3, 4))
	require.Equal(t, uint64(math.MaxUint64), saturatingAdd(math.MaxUint64-1, 2))
}

func TestVerifyManifestStructureRejectsInvalidState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		manifest Manifest
		want     string
	}{
		{
			name:     "initial manifest has coverage",
			manifest: Manifest{AuditWatermark: 1},
			want:     "initial manifest contains published coverage",
		},
		{
			name:     "effective floor",
			manifest: Manifest{Version: 1, EffectiveFloor: 1},
			want:     "non-zero history floor is unsupported",
		},
		{
			name: "invalid reducer state",
			manifest: Manifest{
				Version:      1,
				ReducerState: domainhistory.State{HasLast: true},
			},
			want: "invalid reducer state",
		},
		{
			name: "reducer cursor ahead",
			manifest: Manifest{
				Version:        1,
				AuditWatermark: 1,
				LogWatermark:   1,
				ReducerState: domainhistory.State{
					HasLast: true,
					Last:    domainhistory.Position{AuditSequence: 2, LogSequence: 2},
				},
			},
			want: "reducer cursor exceeds manifest watermarks",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorContains(t, verifyManifestStructure(test.manifest), test.want)
		})
	}
	require.NoError(t, verifyManifestStructure(Manifest{Version: 1}))
}

func TestVerifyRunDescriptorRejectsMalformedDescriptors(t *testing.T) {
	t.Parallel()

	manifest, valid := validVerificationManifestAndRun()
	tests := []struct {
		name   string
		mutate func(*RunRef)
		want   string
	}{
		{name: "zero id", mutate: func(run *RunRef) { run.ID = 0 }, want: "run id zero"},
		{name: "zero first audit", mutate: func(run *RunRef) { run.FirstAuditSequence = 0 }, want: "invalid audit coverage"},
		{name: "descending audit", mutate: func(run *RunRef) { run.FirstAuditSequence = 3 }, want: "invalid audit coverage"},
		{name: "audit beyond manifest", mutate: func(run *RunRef) { run.LastAuditSequence = 3 }, want: "invalid audit coverage"},
		{name: "zero log", mutate: func(run *RunRef) { run.MaxLogSequence = 0 }, want: "invalid log coverage"},
		{name: "log beyond manifest", mutate: func(run *RunRef) { run.MaxLogSequence = 3 }, want: "invalid log coverage"},
		{name: "zero entries", mutate: func(run *RunRef) { run.EntryCount = 0 }, want: "is empty"},
		{name: "zero identities", mutate: func(run *RunRef) { run.IdentityCount = 0 }, want: "is empty"},
		{name: "empty checksum", mutate: func(run *RunRef) { run.Checksum = [32]byte{} }, want: "empty checksum"},
		{name: "removed without archive", mutate: func(run *RunRef) { run.LocalRemoved = true }, want: "removed locally without an archive"},
		{name: "archived without parts", mutate: func(run *RunRef) { run.Archived = true }, want: "archived without bounded parts"},
		{
			name: "parts without archive",
			mutate: func(run *RunRef) {
				run.ArchiveParts = []ArchivePart{{}}
			},
			want: "archive parts without archived state",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			run := valid
			test.mutate(&run)
			require.ErrorContains(t, verifyRunDescriptor(manifest, run), test.want)
		})
	}
	require.NoError(t, verifyRunDescriptor(manifest, valid))
}

func TestVerifyArchivePartsRejectsInvalidCoverage(t *testing.T) {
	t.Parallel()

	_, base := validVerificationManifestAndRun()
	base.Archived = true
	lower := runPrefix(prefixRunData, base.ID)
	upper := prefixEnd(runPrefix(prefixRunCatalog, base.ID))
	base.ArchiveParts = []ArchivePart{validArchivePart(base.ID, 2, lower, upper)}
	require.NoError(t, verifyArchiveParts(base))

	tests := []struct {
		name   string
		mutate func(*RunRef)
		want   string
	}{
		{
			name: "invalid reference",
			mutate: func(run *RunRef) {
				run.ArchiveParts[0].Ref.Version = 0
			},
			want: "invalid reference",
		},
		{
			name: "invalid bounds",
			mutate: func(run *RunRef) {
				run.ArchiveParts[0].UpperBound = run.ArchiveParts[0].LowerBound
			},
			want: "invalid bounds",
		},
		{
			name: "outside keyspace",
			mutate: func(run *RunRef) {
				run.ArchiveParts[0].LowerBound = []byte{0}
			},
			want: "outside the run keyspace",
		},
		{
			name: "does not cover end",
			mutate: func(run *RunRef) {
				run.ArchiveParts[0].UpperBound = runPrefix(prefixRunCatalog, run.ID)
			},
			want: "do not cover the end",
		},
		{
			name: "record count mismatch",
			mutate: func(run *RunRef) {
				run.ArchiveParts[0].Ref.RecordCount = 1
			},
			want: "archive record count is 1, want 2",
		},
		{
			name: "non-contiguous",
			mutate: func(run *RunRef) {
				split := runPrefix(prefixRunCatalog, run.ID)
				run.ArchiveParts = []ArchivePart{
					validArchivePart(run.ID, 1, lower, split),
					validArchivePart(run.ID, 1, append(split, 0), upper),
				}
			},
			want: "are not contiguous",
		},
		{
			name: "record count overflow",
			mutate: func(run *RunRef) {
				split := runPrefix(prefixRunCatalog, run.ID)
				run.ArchiveParts = []ArchivePart{
					validArchivePart(run.ID, math.MaxUint64, lower, split),
					validArchivePart(run.ID, 1, split, upper),
				}
			},
			want: "record count overflows",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			run := cloneRunRef(base)
			test.mutate(&run)
			require.ErrorContains(t, verifyArchiveParts(run), test.want)
		})
	}
}

func TestRunRecordVerifierRejectsMalformedRecords(t *testing.T) {
	t.Parallel()

	identity := recordIdentity{
		Axis:           AxisEffective,
		Scope:          scopeVolume,
		LedgerID:       7,
		Account:        "assets:cash",
		AssetBase:      "USD",
		AssetPrecision: 2,
	}
	catalog, err := catalogKey(1, identity)
	require.NoError(t, err)
	dataOne, err := dataKey(1, identity, 1)
	require.NoError(t, err)
	dataTwo, err := dataKey(1, identity, 2)
	require.NoError(t, err)
	value := encodeCumulative(cumulativeValue{input: big.NewInt(1), output: big.NewInt(0)})

	tests := []struct {
		name string
		run  func(*runRecordVerifier) error
		want string
	}{
		{
			name: "unordered",
			run: func(verifier *runRecordVerifier) error {
				require.NoError(t, verifier.add(catalog, nil))

				return verifier.add(catalog, nil)
			},
			want: "not strictly ordered",
		},
		{
			name: "catalog for another run",
			run: func(verifier *runRecordVerifier) error {
				other, keyErr := catalogKey(2, identity)
				require.NoError(t, keyErr)

				return verifier.add(other, nil)
			},
			want: "catalog key for another run",
		},
		{
			name: "invalid catalog key",
			run: func(verifier *runRecordVerifier) error {
				return verifier.add(runPrefix(prefixRunCatalog, 1), nil)
			},
			want: "invalid catalog key",
		},
		{
			name: "non-empty catalog value",
			run: func(verifier *runRecordVerifier) error {
				return verifier.add(catalog, []byte{1})
			},
			want: "catalog value is not empty",
		},
		{
			name: "data for another run",
			run: func(verifier *runRecordVerifier) error {
				other, keyErr := dataKey(2, identity, 1)
				require.NoError(t, keyErr)

				return verifier.add(other, value)
			},
			want: "invalid data key",
		},
		{
			name: "invalid cumulative value",
			run: func(verifier *runRecordVerifier) error {
				return verifier.add(dataOne, []byte{1})
			},
			want: "invalid cumulative value",
		},
		{
			name: "non-increasing timeline",
			run: func(verifier *runRecordVerifier) error {
				require.NoError(t, verifier.add(dataOne, value))

				return verifier.add(dataTwo, value)
			},
			want: "did not strictly increase",
		},
		{
			name: "outside known prefixes",
			run: func(verifier *runRecordVerifier) error {
				return verifier.add([]byte("unknown"), nil)
			},
			want: "outside data and catalog",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := test.run(newRunRecordVerifier(1))
			require.ErrorContains(t, err, test.want)
		})
	}

	verifier := newRunRecordVerifier(1)
	require.NoError(t, verifier.add(catalog, nil))
	_, _, _, err = verifier.finish()
	require.ErrorContains(t, err, "catalog/data identity cardinality differs")
}

func TestMapArchiveErrorAndRunCoverage(t *testing.T) {
	t.Parallel()

	require.NoError(t, mapArchiveError(nil))
	missing := &balancehistoryarchive.MissingError{}
	var sourceMissing *ErrSourceMissing
	require.ErrorAs(t, mapArchiveError(missing), &sourceMissing)
	corrupt := &balancehistoryarchive.CorruptError{}
	var storeCorrupt *ErrCorrupt
	require.ErrorAs(t, mapArchiveError(corrupt), &storeCorrupt)
	wantErr := errors.New("transport")
	require.ErrorIs(t, mapArchiveError(wantErr), wantErr)

	require.NoError(t, verifyRunCoverage([]RunRef{
		{ID: 2, FirstAuditSequence: 3, LastAuditSequence: 4},
		{ID: 1, FirstAuditSequence: 1, LastAuditSequence: 2},
	}))
	err := verifyRunCoverage([]RunRef{
		{ID: 1, FirstAuditSequence: 1, LastAuditSequence: 3},
		{ID: 2, FirstAuditSequence: 3, LastAuditSequence: 4},
	})
	require.ErrorContains(t, err, "overlapping audit coverage")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store := newTestStore(t)
	err = store.VerifyContext(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.VerifyBoundedContext(context.Background(), 0, 0)
	require.ErrorContains(t, err, "maximum archive parts must be positive")
}
