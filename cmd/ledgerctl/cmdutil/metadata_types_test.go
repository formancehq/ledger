package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestParseTargetType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    ledgerpb.TargetType
		wantErr bool
	}{
		{"account", "account", ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, false},
		{"transaction", "transaction", ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, false},
		{"ledger", "ledger", ledgerpb.TargetType_TARGET_TYPE_LEDGER, false},
		{"Account uppercase", "Account", ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, false},
		{"TRANSACTION uppercase", "TRANSACTION", ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, false},
		{"LEDGER uppercase", "LEDGER", ledgerpb.TargetType_TARGET_TYPE_LEDGER, false},
		{"invalid", "unknown", 0, true},
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
		want    ledgerpb.MetadataType
		wantErr bool
	}{
		{"string", "string", ledgerpb.MetadataType_METADATA_TYPE_STRING, false},
		{"int64", "int64", ledgerpb.MetadataType_METADATA_TYPE_INT64, false},
		{"bool", "bool", ledgerpb.MetadataType_METADATA_TYPE_BOOL, false},
		{"uint64", "uint64", ledgerpb.MetadataType_METADATA_TYPE_UINT64, false},
		{"int8", "int8", ledgerpb.MetadataType_METADATA_TYPE_INT8, false},
		{"int16", "int16", ledgerpb.MetadataType_METADATA_TYPE_INT16, false},
		{"int32", "int32", ledgerpb.MetadataType_METADATA_TYPE_INT32, false},
		{"uint8", "uint8", ledgerpb.MetadataType_METADATA_TYPE_UINT8, false},
		{"uint16", "uint16", ledgerpb.MetadataType_METADATA_TYPE_UINT16, false},
		{"uint32", "uint32", ledgerpb.MetadataType_METADATA_TYPE_UINT32, false},
		{"Bool uppercase", "Bool", ledgerpb.MetadataType_METADATA_TYPE_BOOL, false},
		{"INT64 uppercase", "INT64", ledgerpb.MetadataType_METADATA_TYPE_INT64, false},
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
		input ledgerpb.MetadataType
		want  string
	}{
		{ledgerpb.MetadataType_METADATA_TYPE_STRING, "string"},
		{ledgerpb.MetadataType_METADATA_TYPE_INT64, "int64"},
		{ledgerpb.MetadataType_METADATA_TYPE_BOOL, "bool"},
		{ledgerpb.MetadataType_METADATA_TYPE_UINT64, "uint64"},
		{ledgerpb.MetadataType_METADATA_TYPE_INT8, "int8"},
		{ledgerpb.MetadataType_METADATA_TYPE_INT16, "int16"},
		{ledgerpb.MetadataType_METADATA_TYPE_INT32, "int32"},
		{ledgerpb.MetadataType_METADATA_TYPE_UINT8, "uint8"},
		{ledgerpb.MetadataType_METADATA_TYPE_UINT16, "uint16"},
		{ledgerpb.MetadataType_METADATA_TYPE_UINT32, "uint32"},
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

	require.Equal(t, "account", TargetTypeString(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT))
	require.Equal(t, "transaction", TargetTypeString(ledgerpb.TargetType_TARGET_TYPE_TRANSACTION))
}

func TestMetadataTypeOptions(t *testing.T) {
	t.Parallel()

	opts := MetadataTypeOptions()
	require.Len(t, opts, 11)
	require.Contains(t, opts, "string")
	require.Contains(t, opts, "int64")
	require.Contains(t, opts, "bool")
	require.Contains(t, opts, "uint64")
	require.Contains(t, opts, "datetime")
}

func TestTargetTypeOptions(t *testing.T) {
	t.Parallel()

	opts := TargetTypeOptions()
	require.Len(t, opts, 3)
	require.Contains(t, opts, "account")
	require.Contains(t, opts, "transaction")
	require.Contains(t, opts, "ledger")
}

func TestParseSchemaEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		wantTarget ledgerpb.TargetType
		wantKey    string
		wantType   ledgerpb.MetadataType
		wantErr    bool
	}{
		{
			name:       "account int64",
			input:      "account:age:int64",
			wantTarget: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
			wantKey:    "age",
			wantType:   ledgerpb.MetadataType_METADATA_TYPE_INT64,
		},
		{
			name:       "transaction bool",
			input:      "transaction:active:bool",
			wantTarget: ledgerpb.TargetType_TARGET_TYPE_TRANSACTION,
			wantKey:    "active",
			wantType:   ledgerpb.MetadataType_METADATA_TYPE_BOOL,
		},
		{
			name:       "account uint64",
			input:      "account:count:uint64",
			wantTarget: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
			wantKey:    "count",
			wantType:   ledgerpb.MetadataType_METADATA_TYPE_UINT64,
		},
		{
			name:    "missing parts",
			input:   "account:age",
			wantErr: true,
		},
		{
			name:       "ledger string",
			input:      "ledger:env:string",
			wantTarget: ledgerpb.TargetType_TARGET_TYPE_LEDGER,
			wantKey:    "env",
			wantType:   ledgerpb.MetadataType_METADATA_TYPE_STRING,
		},
		{
			name:    "invalid target",
			input:   "unknown:age:int64",
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
			wantTarget: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
			wantKey:    "user.age",
			wantType:   ledgerpb.MetadataType_METADATA_TYPE_INT32,
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

func TestParseSchemaEntryPreservesColonInKey(t *testing.T) {
	t.Parallel()
	_, key, _, err := ParseSchemaEntry("transaction:external:id:string")
	require.NoError(t, err)
	require.Equal(t, "external:id", key)
}
