package publicpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	grpc "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestLedgerLogCategoryCompleteness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		payload  *grpc.LedgerLogPayload
		category LedgerLogCategory
	}{
		{"created transaction", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_CreatedTransaction{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY},
		{"reverted transaction", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_RevertedTransaction{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY},
		{"saved metadata", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_SavedMetadata{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY},
		{"deleted metadata", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_DeletedMetadata{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY},
		{"set metadata field type", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_SetMetadataFieldType{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"removed metadata field type", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_RemovedMetadataFieldType{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"filled gap", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_FillGap{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"created index", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_CreateIndex{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"dropped index", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_DropIndex{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"added account type", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_AddedAccountType{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"removed account type", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_RemovedAccountType{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"updated default enforcement mode", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_UpdatedDefaultEnforcementMode{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_CONTROL},
		{"order skipped", &grpc.LedgerLogPayload{Payload: &grpc.LedgerLogPayload_OrderSkipped{}}, LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY},
	}

	descriptor := (&grpc.LedgerLogPayload{}).ProtoReflect().Descriptor()
	oneof := descriptor.Oneofs().ByName("payload")
	require.NotNil(t, oneof)
	require.Equal(t, oneof.Fields().Len(), len(tests),
		"a new grpc.LedgerLogPayload arm must be added to this exhaustive behavior table")

	seenFields := make(map[string]struct{}, len(tests))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field := test.payload.ProtoReflect().WhichOneof(oneof)
			require.NotNil(t, field)
			_, duplicate := seenFields[string(field.FullName())]
			assert.False(t, duplicate, "oneof arm covered more than once")
			seenFields[string(field.FullName())] = struct{}{}

			assert.Equal(t, test.category, LedgerLogCategoryOf(test.payload))
			assert.Equal(t, test.category == LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY,
				IsLedgerHistoryPayload(test.payload))
		})
	}

	assert.Equal(t, LedgerLogCategory_LEDGER_LOG_CATEGORY_UNSPECIFIED, LedgerLogCategoryOf(nil))
	assert.Equal(t, LedgerLogCategory_LEDGER_LOG_CATEGORY_UNSPECIFIED, LedgerLogCategoryOf(&grpc.LedgerLogPayload{}))
}
