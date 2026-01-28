package ledgerpb

import "google.golang.org/protobuf/proto"

// LogPayloadInterface represents a log payload that can be converted to protobuf LogPayload
// Type() returns the log type as int32 (matching protobuf Log.Type field)
type LogPayloadInterface interface {
	proto.Message
	Type() int32
}

// Type returns the log type for CreatedTransaction
func (ct *CreatedTransaction) Type() int32 {
	return 1 // NewTransactionLogType
}

// Type returns the log type for RevertedTransaction
func (rt *RevertedTransaction) Type() int32 {
	return 2 // RevertedTransactionLogType
}

// Type returns the log type for SavedMetadata
func (sm *SavedMetadata) Type() int32 {
	return 0 // SetMetadataLogType
}

// Type returns the log type for DeletedMetadata
func (dm *DeletedMetadata) Type() int32 {
	return 3 // DeleteMetadataLogType
}

