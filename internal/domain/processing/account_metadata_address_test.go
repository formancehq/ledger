package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestProcessOrder_AccountMetadataAddressFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		addresses []string
		want      error
	}{
		{"empty", []string{""}, domain.ErrAccountAddressEmpty},
		{"invalid", []string{"bad!"}, domain.ErrAccountAddressInvalidChar},
		{"competing-empty", []string{"bad!", ""}, domain.ErrAccountAddressEmpty},
		{"competing-nonempty", []string{"a::b", "a b"}, domain.ErrAccountAddressInvalidChar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			scope := metadataCommandScope(t, ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)
			// Script metadata and caller metadata are merged before addresses
			// are checked. The caller's accepted business payload must stay intact.
			order := metadataScriptOrder(t, `set_account_meta(@users:alice, "k", "v")`)
			metadata := make(map[string]*commonpb.MetadataMap)
			for _, address := range tc.addresses {
				metadata[address] = &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}}
			}
			order.GetLedgerScoped().GetApply().GetCreateTransaction().AccountMetadata = metadata
			before := proto.Clone(order)
			result, procErr := processor.ProcessOrder(order, scope)
			require.Nil(t, result)
			require.ErrorIs(t, procErr, tc.want)
			require.True(t, proto.Equal(before, order), "rejection must preserve accepted order bytes")
		})
	}
}
