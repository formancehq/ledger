package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateInstanceID_Length(t *testing.T) {
	t.Parallel()

	id, err := GenerateInstanceID()
	require.NoError(t, err)
	require.Len(t, id, InstanceIDLen)
}

func TestGenerateInstanceID_Uniqueness(t *testing.T) {
	t.Parallel()

	a, err := GenerateInstanceID()
	require.NoError(t, err)

	b, err := GenerateInstanceID()
	require.NoError(t, err)

	require.NotEqual(t, a, b, "two consecutive GenerateInstanceID calls must not produce the same value")
}

func TestWriteAndReadInstanceID_Roundtrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	id, err := GenerateInstanceID()
	require.NoError(t, err)

	require.NoError(t, WriteInstanceID(dir, id))

	got, err := ReadInstanceID(dir)
	require.NoError(t, err)
	require.Equal(t, id, got)
}

func TestReadInstanceID_AbsentReturnsNilNoError(t *testing.T) {
	t.Parallel()

	got, err := ReadInstanceID(t.TempDir())
	require.NoError(t, err)
	require.Nil(t, got, "absent marker must be signalled as (nil, nil) so callers can decide legacy behaviour")
}

func TestReadInstanceID_WrongLengthIsFatal(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, InstanceIDMarkerFile), []byte("too-short"), 0o600))

	_, err := ReadInstanceID(dir)
	require.Error(t, err, "a corrupt marker (wrong length) must surface as an error, never silently truncate")
}

// TestWriteInstanceID_RefusesOverwrite pins the immutability guarantee: once
// a peer has an instance id, it never changes for the lifetime of the WAL.
// Overwriting would defeat the discrimination property EN-1045 relies on.
func TestWriteInstanceID_RefusesOverwrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := GenerateInstanceID()
	require.NoError(t, err)
	require.NoError(t, WriteInstanceID(dir, first))

	second, err := GenerateInstanceID()
	require.NoError(t, err)
	require.Error(t, WriteInstanceID(dir, second), "WriteInstanceID must refuse to overwrite an existing marker")

	got, err := ReadInstanceID(dir)
	require.NoError(t, err)
	require.Equal(t, first, got, "the original marker must survive the failed overwrite")
}

func TestEnsureInstanceID_GeneratesOnFirstCall(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	id, err := EnsureInstanceID(dir)
	require.NoError(t, err)
	require.Len(t, id, InstanceIDLen)

	// Second call in the same directory must return the same value.
	again, err := EnsureInstanceID(dir)
	require.NoError(t, err)
	require.Equal(t, id, again, "EnsureInstanceID must be idempotent per WAL directory")
}

func TestEnsureInstanceID_CreatesMissingDirectory(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	nested := filepath.Join(base, "nested", "wal-dir")

	id, err := EnsureInstanceID(nested)
	require.NoError(t, err)
	require.Len(t, id, InstanceIDLen)

	// Marker exists on disk.
	got, err := ReadInstanceID(nested)
	require.NoError(t, err)
	require.Equal(t, id, got)
}

func TestWriteInstanceID_RejectsWrongLength(t *testing.T) {
	t.Parallel()

	require.Error(t, WriteInstanceID(t.TempDir(), []byte("short")))
	require.Error(t, WriteInstanceID(t.TempDir(), make([]byte, InstanceIDLen+1)))
}
