package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestVtFallbackCodec_Name(t *testing.T) {
	t.Parallel()

	codec := vtFallbackCodec{}
	require.Equal(t, "proto", codec.Name())
}

func TestVtFallbackCodec_Marshal_NonVTMessage(t *testing.T) {
	t.Parallel()

	// wrapperspb.StringValue is a standard proto.Message without MarshalVT
	msg := wrapperspb.String("hello")

	codec := vtFallbackCodec{}
	data, err := codec.Marshal(msg)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify round-trip
	out := &wrapperspb.StringValue{}
	require.NoError(t, proto.Unmarshal(data, out))
	require.Equal(t, "hello", out.GetValue())
}

func TestVtFallbackCodec_Marshal_NonProtoMessage(t *testing.T) {
	t.Parallel()

	codec := vtFallbackCodec{}
	_, err := codec.Marshal("not a proto message")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to marshal")
	require.Contains(t, err.Error(), "is not a proto.Message")
}

func TestVtFallbackCodec_Unmarshal_NonVTMessage(t *testing.T) {
	t.Parallel()

	// Marshal with standard proto first
	msg := wrapperspb.String("world")
	data, err := proto.Marshal(msg)
	require.NoError(t, err)

	// Unmarshal via codec
	codec := vtFallbackCodec{}
	out := &wrapperspb.StringValue{}
	require.NoError(t, codec.Unmarshal(data, out))
	require.Equal(t, "world", out.GetValue())
}

func TestVtFallbackCodec_Unmarshal_NonProtoMessage(t *testing.T) {
	t.Parallel()

	codec := vtFallbackCodec{}
	var s string
	err := codec.Unmarshal([]byte{0x0a, 0x05}, &s)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal")
	require.Contains(t, err.Error(), "is not a proto.Message")
}
