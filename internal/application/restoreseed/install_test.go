package restoreseed

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
)

func TestColdReadIndexStage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		checkpoint *backup.CheckpointManifest
	}{
		{"legacy", nil}, {"unknown version", &backup.CheckpointManifest{ReadIndex: &backup.ReadIndexManifest{Version: 321}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, index := t.TempDir(), t.TempDir()
			require.NoError(t, reserveDirectory(data, stageDir, ownerFile, ownerTag, "readindex"))
			require.NoError(t, os.WriteFile(filepath.Join(seedPath(data), "stale.sst"), []byte("old attempt"), 0o600))
			id, err := Stage(t.Context(), data, "bucket", nil, tc.checkpoint, nil, backup.ReadIndexDownloadOptions{Parallelism: 1}, logging.NopZap())
			require.NoError(t, err)
			target := filepath.Join(index, "readindex")
			require.NoError(t, os.Mkdir(target, 0o700))
			file := filepath.Join(target, "foreign.sst")
			require.NoError(t, os.WriteFile(file, []byte("unrelated"), 0o600))
			require.ErrorContains(t, Install(data, index, id), "does not belong to this restore")
			require.FileExists(t, file)
			require.NoError(t, os.Remove(file))
			require.NoError(t, Install(data, index, id)) // empty dir and retry
			require.NoFileExists(t, filepath.Join(target, "stale.sst"))
			require.NoDirExists(t, filepath.Join(data, stageDir))
			require.True(t, matchesGeneration(target, id))
			require.NoError(t, os.WriteFile(file, []byte("new local writes"), 0o600))
			require.NoError(t, Install(data, index, id)) // restart before WAL creation
			got, err := os.ReadFile(file)
			require.NoError(t, err)
			require.Equal(t, "new local writes", string(got))
		})
	}
}

func TestInstallRestoreSeedVolumeCopyRetry(t *testing.T) {
	t.Parallel()
	src, index := t.TempDir(), t.TempDir()
	const id = "10f5c74a-bf6f-4e3f-be15-21b87366a999"
	require.NoError(t, os.WriteFile(filepath.Join(src, generationFile), []byte(id), 0o600))
	input := []byte("immutable source SST")
	require.NoError(t, os.WriteFile(filepath.Join(src, "00001.sst"), input, 0o600))
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(src, "zzz")))
	require.ErrorContains(t, copyAcrossDevices(src, index, id), "unexpected file type")
	target := filepath.Join(index, "readindex")
	require.NoDirExists(t, target)
	tmp := filepath.Join(index, ".restore-"+id+".tmp")
	staged, err := os.Stat(filepath.Join(tmp, "00001.sst"))
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(tmp, ".restore-copy-partial"))
	require.NoError(t, os.Remove(filepath.Join(src, "zzz")))
	require.NoError(t, copyAcrossDevices(src, index, id))
	got, err := os.ReadFile(filepath.Join(target, "00001.sst"))
	require.NoError(t, err)
	require.Equal(t, input, got)
	final, err := os.Stat(filepath.Join(target, "00001.sst"))
	require.NoError(t, err)
	require.True(t, os.SameFile(staged, final), "retry must reuse the completed SST, not copy it again")
	require.True(t, matchesGeneration(target, id))
	require.NoDirExists(t, tmp)
}

func TestValidateReadIndexLocationRejectsPrimaryAndStageAliases(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	require.NoError(t, ValidateLocation(data, filepath.Join(data, "read-indexes")))
	for _, dir := range []string{"live/new", "checkpoints/0/index", stageDir + "/nested", "tmp/reindexed"} {
		require.ErrorContains(t, ValidateLocation(data, filepath.Join(data, dir)), "reserved")
	}
	volume := t.TempDir()
	require.NoError(t, os.Symlink(volume, filepath.Join(data, "live")))
	link := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(volume, link))
	require.ErrorContains(t, ValidateLocation(data, filepath.Join(link, "new")), "reserved live directory")
	require.NoError(t, os.Symlink(t.TempDir(), filepath.Join(volume, "away")))
	require.ErrorContains(t, ValidateLocation(data, filepath.Join(data, "live", "away", "new")), "reserved live directory")
}

func TestStageNeverReclaimsAnUnownedDirectory(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	dir := filepath.Join(data, stageDir)
	require.NoError(t, os.Mkdir(dir, 0o750))
	file := filepath.Join(dir, "unrelated")
	require.NoError(t, os.WriteFile(file, []byte("owned by an older deployment"), 0o600))
	_, err := Stage(t.Context(), data, "bucket", nil, nil, nil, backup.ReadIndexDownloadOptions{Parallelism: 1}, logging.NopZap())
	require.ErrorContains(t, err, "unowned read-index staging")
	require.ErrorContains(t, Discard(data), "unowned read-index staging")
	got, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "owned by an older deployment", string(got))
}
