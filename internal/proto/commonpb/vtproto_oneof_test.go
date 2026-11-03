package commonpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestVTProtoPreservesSelectedNilMessage(t *testing.T) {
	t.Parallel()
	// A selected nil message is an empty message on the wire, not an absent
	// oneof. Older generators omit it, changing persisted and audit bytes.
	for name, message := range map[string]interface {
		proto.Message
		SizeVT() int
		MarshalToSizedBufferVT([]byte) (int, error)
	}{
		"metadata null":        &commonpb.MetadataValue{Type: &commonpb.MetadataValue_NullValue{}},
		"account metadata log": &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AccountMetadataUpdated{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want, err := proto.Marshal(message)
			require.NoError(t, err)
			require.NotEmpty(t, want)
			buffer := make([]byte, message.SizeVT())
			written, err := message.MarshalToSizedBufferVT(buffer)
			require.NoError(t, err)
			require.Equal(t, want, buffer[len(buffer)-written:])
		})
	}
}
