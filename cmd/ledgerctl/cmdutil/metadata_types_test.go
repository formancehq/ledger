package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestParseTargetType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    commonpb.TargetType
		wantErr bool
	}{
		{"account", "account", commonpb.TargetType_TARGET_TYPE_ACCOUNT, false},
		{"transaction", "transaction", commonpb.TargetType_TARGET_TYPE_TRANSACTION, false},
		{"Account uppercase", "Account", commonpb.TargetType_TARGET_TYPE_ACCOUNT, false},
		{"TRANSACTION uppercase", "TRANSACTION", commonpb.TargetType_TARGET_TYPE_TRANSACTION, false},
		{"invalid", "ledger", 0, true},
		{"empty", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseTargetType(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseMetadataType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    commonpb.MetadataType
		wantErr bool
	}{
		{"string", "string", commonpb.MetadataType_METADATA_TYPE_STRING, false},
		{"int64", "int64", commonpb.MetadataType_METADATA_TYPE_INT64, false},
		{"bool", "bool", commonpb.MetadataType_METADATA_TYPE_BOOL, false},
		{"uint64", "uint64", commonpb.MetadataType_METADATA_TYPE_UINT64, false},
		{"int8", "int8", commonpb.MetadataType_METADATA_TYPE_INT8, false},
		{"int16", "int16", commonpb.MetadataType_METADATA_TYPE_INT16, false},
		{"int32", "int32", commonpb.MetadataType_METADATA_TYPE_INT32, false},
		{"uint8", "uint8", commonpb.MetadataType_METADATA_TYPE_UINT8, false},
		{"uint16", "uint16", commonpb.MetadataType_METADATA_TYPE_UINT16, false},
		{"uint32", "uint32", commonpb.MetadataType_METADATA_TYPE_UINT32, false},
		{"Bool uppercase", "Bool", commonpb.MetadataType_METADATA_TYPE_BOOL, false},
		{"INT64 uppercase", "INT64", commonpb.MetadataType_METADATA_TYPE_INT64, false},
		{"invalid", "float64", 0, true},
		{"empty", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseMetadataType(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMetadataTypeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input commonpb.MetadataType
		want  string
	}{
		{commonpb.MetadataType_METADATA_TYPE_STRING, "string"},
		{commonpb.MetadataType_METADATA_TYPE_INT64, "int64"},
		{commonpb.MetadataType_METADATA_TYPE_BOOL, "bool"},
		{commonpb.MetadataType_METADATA_TYPE_UINT64, "uint64"},
		{commonpb.MetadataType_METADATA_TYPE_INT8, "int8"},
		{commonpb.MetadataType_METADATA_TYPE_INT16, "int16"},
		{commonpb.MetadataType_METADATA_TYPE_INT32, "int32"},
		{commonpb.MetadataType_METADATA_TYPE_UINT8, "uint8"},
		{commonpb.MetadataType_METADATA_TYPE_UINT16, "uint16"},
		{commonpb.MetadataType_METADATA_TYPE_UINT32, "uint32"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, MetadataTypeString(tt.input))
		})
	}
}

func TestTargetTypeString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "account", TargetTypeString(commonpb.TargetType_TARGET_TYPE_ACCOUNT))
	require.Equal(t, "transaction", TargetTypeString(commonpb.TargetType_TARGET_TYPE_TRANSACTION))
}

func TestMetadataTypeOptions(t *testing.T) {
	t.Parallel()

	opts := MetadataTypeOptions()
	require.Len(t, opts, 10)
	require.Contains(t, opts, "string")
	require.Contains(t, opts, "int64")
	require.Contains(t, opts, "bool")
	require.Contains(t, opts, "uint64")
}

func TestTargetTypeOptions(t *testing.T) {
	t.Parallel()

	opts := TargetTypeOptions()
	require.Len(t, opts, 2)
	require.Contains(t, opts, "account")
	require.Contains(t, opts, "transaction")
}

func TestParseSchemaEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		wantTarget commonpb.TargetType
		wantKey    string
		wantType   commonpb.MetadataType
		wantErr    bool
	}{
		{
			name:       "account int64",
			input:      "account:age:int64",
			wantTarget: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			wantKey:    "age",
			wantType:   commonpb.MetadataType_METADATA_TYPE_INT64,
		},
		{
			name:       "transaction bool",
			input:      "transaction:active:bool",
			wantTarget: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
			wantKey:    "active",
			wantType:   commonpb.MetadataType_METADATA_TYPE_BOOL,
		},
		{
			name:       "account uint64",
			input:      "account:count:uint64",
			wantTarget: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			wantKey:    "count",
			wantType:   commonpb.MetadataType_METADATA_TYPE_UINT64,
		},
		{
			name:    "missing parts",
			input:   "account:age",
			wantErr: true,
		},
		{
			name:    "invalid target",
			input:   "ledger:age:int64",
			wantErr: true,
		},
		{
			name:    "invalid type",
			input:   "account:age:float64",
			wantErr: true,
		},
		{
			name:    "empty key",
			input:   "account::int64",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:       "key with dots",
			input:      "account:user.age:int32",
			wantTarget: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			wantKey:    "user.age",
			wantType:   commonpb.MetadataType_METADATA_TYPE_INT32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target, key, mdType, err := ParseSchemaEntry(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantTarget, target)
			require.Equal(t, tt.wantKey, key)
			require.Equal(t, tt.wantType, mdType)
		})
	}
}

func TestParseMetadataTypeRoundTrip(t *testing.T) {
	t.Parallel()

	// Every type name from MetadataTypeOptions should parse and round-trip back
	for _, name := range MetadataTypeOptions() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mdType, err := ParseMetadataType(name)
			require.NoError(t, err)
			require.Equal(t, name, MetadataTypeString(mdType))
		})
	}
}

func TestParseTargetTypeRoundTrip(t *testing.T) {
	t.Parallel()

	for _, name := range TargetTypeOptions() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			targetType, err := ParseTargetType(name)
			require.NoError(t, err)
			require.Equal(t, name, TargetTypeString(targetType))
		})
	}
}
