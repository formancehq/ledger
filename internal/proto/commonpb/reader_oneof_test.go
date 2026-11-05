package commonpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestMetadataValueReader_ZeroValuesRetainVariant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value *commonpb.MetadataValue
		want  commonpb.MetadataValue_TypeReaderCase
	}{
		{"unset", &commonpb.MetadataValue{}, commonpb.MetadataValue_TypeReaderCase_Unset},
		{"false", &commonpb.MetadataValue{Type: &commonpb.MetadataValue_BoolValue{}}, commonpb.MetadataValue_TypeReaderCase_BoolValue},
		{"zero", &commonpb.MetadataValue{Type: &commonpb.MetadataValue_IntValue{}}, commonpb.MetadataValue_TypeReaderCase_IntValue},
		{"empty string", &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{}}, commonpb.MetadataValue_TypeReaderCase_StringValue},
		{"typed nil scalar", &commonpb.MetadataValue{Type: (*commonpb.MetadataValue_BoolValue)(nil)}, commonpb.MetadataValue_TypeReaderCase_BoolValue},
		{"nil message", &commonpb.MetadataValue{Type: &commonpb.MetadataValue_NullValue{}}, commonpb.MetadataValue_TypeReaderCase_NullValue},
		{"typed nil message", &commonpb.MetadataValue{Type: (*commonpb.MetadataValue_NullValue)(nil)}, commonpb.MetadataValue_TypeReaderCase_NullValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := tc.value.AsReader()
			require.Equal(t, tc.want, r.WhichType())
			require.False(t, r.GetBoolValue())
			require.Zero(t, r.GetIntValue())
			require.Empty(t, r.GetStringValue())
			require.Nil(t, r.GetNullValue())
		})
	}
}
