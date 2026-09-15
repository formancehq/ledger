package ledger

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
)

func TestValidateMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metadata metadata.Metadata
		wantErr  bool
	}{
		{
			name: "at entry count limit",
			metadata: func() metadata.Metadata {
				ret := metadata.Metadata{}
				for i := range MaxMetadataEntries {
					ret[fmt.Sprint(i)] = ""
				}
				return ret
			}(),
		},
		{
			name: "over entry count limit",
			metadata: func() metadata.Metadata {
				ret := metadata.Metadata{}
				for i := range MaxMetadataEntries + 1 {
					ret[fmt.Sprint(i)] = ""
				}
				return ret
			}(),
			wantErr: true,
		},
		{
			name:     "at key size limit",
			metadata: metadata.Metadata{strings.Repeat("k", MaxMetadataKeySize): ""},
		},
		{
			name:     "over key size limit",
			metadata: metadata.Metadata{strings.Repeat("k", MaxMetadataKeySize+1): ""},
			wantErr:  true,
		},
		{
			name:     "at value size limit",
			metadata: metadata.Metadata{"": strings.Repeat("v", MaxMetadataValueSize)},
		},
		{
			name:     "over value size limit",
			metadata: metadata.Metadata{"": strings.Repeat("v", MaxMetadataValueSize+1)},
			wantErr:  true,
		},
		{
			name: "at entity size limit",
			metadata: metadata.Metadata{
				"a": strings.Repeat("v", MaxMetadataValueSize-1),
				"b": strings.Repeat("v", MaxMetadataValueSize-1),
				"c": strings.Repeat("v", MaxMetadataValueSize-1),
				"d": strings.Repeat("v", MaxMetadataValueSize-1),
			},
		},
		{
			name: "over entity size limit",
			metadata: metadata.Metadata{
				"a": strings.Repeat("v", MaxMetadataValueSize),
				"b": strings.Repeat("v", MaxMetadataValueSize),
				"c": strings.Repeat("v", MaxMetadataValueSize),
				"d": strings.Repeat("v", MaxMetadataValueSize),
			},
			wantErr: true,
		},
		{
			name:     "utf8 is counted in bytes",
			metadata: metadata.Metadata{strings.Repeat("é", MaxMetadataKeySize/2+1): ""},
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateMetadata(test.metadata)
			if test.wantErr {
				require.ErrorIs(t, err, ErrMetadataLimitExceeded{})
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateCommandMetadata(t *testing.T) {
	t.Parallel()

	accountMetadata := AccountMetadata{}
	for i := range MaxCommandMetadataSize / MaxMetadataSize {
		accountMetadata[fmt.Sprint(i)] = metadata.Metadata{
			"a": strings.Repeat("v", MaxMetadataValueSize-1),
			"b": strings.Repeat("v", MaxMetadataValueSize-1),
			"c": strings.Repeat("v", MaxMetadataValueSize-1),
			"d": strings.Repeat("v", MaxMetadataValueSize-1),
		}
	}
	require.NoError(t, ValidateCommandMetadata(nil, accountMetadata))

	accountMetadata["extra"] = metadata.Metadata{"key": "value"}
	err := ValidateCommandMetadata(nil, accountMetadata)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrMetadataLimitExceeded{}))
}
