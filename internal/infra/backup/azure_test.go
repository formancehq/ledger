//go:build azure

package backup

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/require"
)

func TestMapDownloadErr(t *testing.T) {
	t.Parallel()

	otherErr := errors.New("boom")

	tests := []struct {
		name         string
		err          error
		wantNotFound bool
	}{
		{
			name:         "blob not found maps to ErrFileNotFound",
			err:          &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)},
			wantNotFound: true,
		},
		{
			name:         "container not found maps to ErrFileNotFound",
			err:          &azcore.ResponseError{ErrorCode: string(bloberror.ContainerNotFound)},
			wantNotFound: true,
		},
		{
			name:         "other response error is preserved",
			err:          &azcore.ResponseError{ErrorCode: string(bloberror.AuthenticationFailed)},
			wantNotFound: false,
		},
		{
			name:         "non-azure error is preserved",
			err:          otherErr,
			wantNotFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := mapDownloadErr("backups/manifest.json", tc.err)
			require.Error(t, got)
			require.Contains(t, got.Error(), "backups/manifest.json")

			if tc.wantNotFound {
				require.ErrorIs(t, got, ErrFileNotFound)
			} else {
				require.NotErrorIs(t, got, ErrFileNotFound)
				require.ErrorIs(t, got, tc.err)
			}
		})
	}
}

func TestMapDownloadErrWrapsOriginal(t *testing.T) {
	t.Parallel()

	// A wrapped (not bare) ResponseError must still be detected via errors.As.
	wrapped := fmt.Errorf("layer: %w", &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)})

	got := mapDownloadErr("key", wrapped)
	require.ErrorIs(t, got, ErrFileNotFound)
}
