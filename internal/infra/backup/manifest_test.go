package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestReadManifest_LegacyFormatSurfacesActionableError verifies that a manifest
// written in the pre-content-addressing schema (checkpoint.files as
// filename→size numbers) is rejected with the typed ErrLegacyManifestFormat
// rather than a cryptic "cannot unmarshal number" JSON error, so an operator
// knows the backup must be retaken (pre-GA break; addresses the NumaryBot
// backward-compat review on PR #1543).
func TestReadManifest_LegacyFormatSurfacesActionableError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)

	legacy := `{"checkpoint":{"timestamp":"t","lastAppliedIndex":1,"lastLogSequence":1,"lastAuditSequence":1,"files":{"000001.sst":123}},"exports":null}`
	storage.EXPECT().GetFile(gomock.Any(), "k").
		Return(io.NopCloser(bytes.NewReader([]byte(legacy))), nil)

	m, err := ReadManifest(context.Background(), storage, "k")
	require.Nil(t, m)
	require.ErrorIs(t, err, ErrLegacyManifestFormat)
}

// TestReadManifest_CurrentFormatDecodes verifies the current filename→{size,key}
// shape still decodes cleanly and is NOT mistaken for legacy.
func TestReadManifest_CurrentFormatDecodes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)

	current := `{"checkpoint":{"timestamp":"t","lastAppliedIndex":1,"lastLogSequence":1,"lastAuditSequence":1,"files":{"000001.sst":{"size":123,"key":"b/backups/data/000001.sst.abc"}}},"exports":null}`
	storage.EXPECT().GetFile(gomock.Any(), "k").
		Return(io.NopCloser(bytes.NewReader([]byte(current))), nil)

	m, err := ReadManifest(context.Background(), storage, "k")
	require.NoError(t, err)
	require.NotNil(t, m.Checkpoint)
	require.Equal(t, "b/backups/data/000001.sst.abc", m.Checkpoint.Files["000001.sst"].Key)
	require.EqualValues(t, 123, m.Checkpoint.Files["000001.sst"].Size)
}

func TestReadManifestOrEmpty_NotFoundReturnsEmpty(t *testing.T) {
	t.Parallel()

	// A genuine not-found (wrapped, as real backends do) is the only case that
	// should start fresh.
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	storage.EXPECT().GetFile(gomock.Any(), "k").
		Return(nil, fmt.Errorf("s3 GetObject k: %w", ErrFileNotFound))

	m, err := ReadManifestOrEmpty(context.Background(), logging.Testing(), storage, "k")
	require.NoError(t, err)
	require.NotNil(t, m)
	require.Nil(t, m.Checkpoint)
	require.Empty(t, m.Exports)
}

func TestReadManifestOrEmpty_CorruptManifestReturnsError(t *testing.T) {
	t.Parallel()

	// A corrupt/undecodable manifest must NOT be masked as an empty manifest —
	// doing so would reset backup history and let later incrementals skip data.
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	storage.EXPECT().GetFile(gomock.Any(), "k").
		Return(io.NopCloser(bytes.NewReader([]byte("{ this is not valid json"))), nil)

	m, err := ReadManifestOrEmpty(context.Background(), logging.Testing(), storage, "k")
	require.Error(t, err)
	require.Nil(t, m)
	require.NotErrorIs(t, err, ErrFileNotFound)
}

func TestReadManifestOrEmpty_GenericErrorReturnsError(t *testing.T) {
	t.Parallel()

	// A transient storage/permission/network error must surface, not start fresh.
	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	storage.EXPECT().GetFile(gomock.Any(), "k").
		Return(nil, errors.New("connection reset by peer"))

	m, err := ReadManifestOrEmpty(context.Background(), logging.Testing(), storage, "k")
	require.Error(t, err)
	require.Nil(t, m)
}
