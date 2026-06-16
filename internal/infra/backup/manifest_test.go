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
