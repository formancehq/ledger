package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// fakeStorage is a test Storage whose GetFile behavior is fully scripted, so we
// can exercise not-found, corrupt-content, and generic-error paths.
type fakeStorage struct {
	getBody []byte
	getErr  error
}

func (f *fakeStorage) PutFile(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}

func (f *fakeStorage) GetFile(_ context.Context, _ string) (io.ReadCloser, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}

	return io.NopCloser(bytes.NewReader(f.getBody)), nil
}

func (f *fakeStorage) DeleteFile(_ context.Context, _ string) error {
	return nil
}

func TestReadManifestOrEmpty_NotFoundReturnsEmpty(t *testing.T) {
	t.Parallel()

	// A genuine not-found (wrapped, as real backends do) is the only case that
	// should start fresh.
	storage := &fakeStorage{getErr: fmt.Errorf("s3 GetObject k: %w", ErrFileNotFound)}

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
	storage := &fakeStorage{getBody: []byte("{ this is not valid json")}

	m, err := ReadManifestOrEmpty(context.Background(), logging.Testing(), storage, "k")
	require.Error(t, err)
	require.Nil(t, m)
	require.NotErrorIs(t, err, ErrFileNotFound)
}

func TestReadManifestOrEmpty_GenericErrorReturnsError(t *testing.T) {
	t.Parallel()

	// A transient storage/permission/network error must surface, not start fresh.
	storage := &fakeStorage{getErr: errors.New("connection reset by peer")}

	m, err := ReadManifestOrEmpty(context.Background(), logging.Testing(), storage, "k")
	require.Error(t, err)
	require.Nil(t, m)
}
