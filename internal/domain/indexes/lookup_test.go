package indexes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func ref() *commonpb.IndexID {
	return indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
}

func meta(key string) *commonpb.IndexID {
	return indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
}

func TestPutFindRemove(t *testing.T) {
	t.Parallel()

	info := &commonpb.LedgerInfo{Name: "test"}

	// Empty: Find returns nil, IsReady false.
	assert.Nil(t, indexes.Find(info, ref()))
	assert.False(t, indexes.IsReady(info, ref()))
	assert.Equal(t, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED, indexes.Status(info, ref()))

	// Put then Find.
	idx := &commonpb.Index{Id: ref(), BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING}
	indexes.Put(info, idx)
	require.Len(t, info.GetIndexes(), 1)

	found := indexes.Find(info, ref())
	require.NotNil(t, found)
	assert.Equal(t, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING, found.GetBuildStatus())

	// IsReady false while BUILDING.
	assert.False(t, indexes.IsReady(info, ref()))

	// Put again with same id: in-place update, no duplicate.
	idx2 := &commonpb.Index{Id: ref(), BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY}
	indexes.Put(info, idx2)
	require.Len(t, info.GetIndexes(), 1)
	assert.True(t, indexes.IsReady(info, ref()))

	// Put a different id: appended.
	indexes.Put(info, &commonpb.Index{Id: meta("color"), BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING})
	require.Len(t, info.GetIndexes(), 2)

	// Remove one: the other survives.
	assert.True(t, indexes.Remove(info, ref()))
	require.Len(t, info.GetIndexes(), 1)
	assert.Nil(t, indexes.Find(info, ref()))
	assert.NotNil(t, indexes.Find(info, meta("color")))

	// Remove unknown: false, no change.
	assert.False(t, indexes.Remove(info, ref()))
	require.Len(t, info.GetIndexes(), 1)

	// Remove last: empty.
	assert.True(t, indexes.Remove(info, meta("color")))
	assert.Empty(t, info.GetIndexes())
}

func TestNilSafe(t *testing.T) {
	t.Parallel()

	assert.Nil(t, indexes.Find(nil, ref()))
	assert.Nil(t, indexes.Find(&commonpb.LedgerInfo{}, nil))
	assert.False(t, indexes.IsReady(nil, ref()))
	assert.False(t, indexes.Remove(nil, ref()))

	// Put with nil idx is a no-op.
	info := &commonpb.LedgerInfo{}
	indexes.Put(info, nil)
	assert.Empty(t, info.GetIndexes())
}
