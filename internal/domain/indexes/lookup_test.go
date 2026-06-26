package indexes_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func ref() *commonpb.IndexID {
	return indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
}

func TestFind_ReturnsValueOnHit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reader := NewMockLookup(ctrl)

	key := indexes.KeyFor("main", ref())
	expected := (&commonpb.Index{Id: ref(), BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING}).AsReader()

	reader.EXPECT().Get(key).Return(expected, nil)

	got, err := indexes.Find(reader, "main", ref())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING, got.GetBuildStatus())
}

func TestFind_ErrNotFoundCollapsesToNilNoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reader := NewMockLookup(ctrl)

	reader.EXPECT().Get(indexes.KeyFor("main", ref())).Return(nil, domain.ErrNotFound)

	got, err := indexes.Find(reader, "main", ref())
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestFind_PropagatesOtherErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reader := NewMockLookup(ctrl)

	boom := errors.New("coverage miss")
	reader.EXPECT().Get(indexes.KeyFor("main", ref())).Return(nil, boom)

	got, err := indexes.Find(reader, "main", ref())
	require.ErrorIs(t, err, boom)
	assert.Nil(t, got)
}

func TestFind_NilReaderIsNoOp(t *testing.T) {
	t.Parallel()

	got, err := indexes.Find(nil, "main", ref())
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestFind_NilIDIsNoOp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reader := NewMockLookup(ctrl)
	// No EXPECT — Find must short-circuit before reaching GetIndex.

	got, err := indexes.Find(reader, "main", nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestIsReady_TrueOnlyForReadyStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reader := NewMockLookup(ctrl)

	key := indexes.KeyFor("main", ref())

	gomock.InOrder(
		reader.EXPECT().Get(key).Return((&commonpb.Index{Id: ref(), BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING}).AsReader(), nil),
		reader.EXPECT().Get(key).Return((&commonpb.Index{Id: ref(), BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY}).AsReader(), nil),
		reader.EXPECT().Get(key).Return(nil, domain.ErrNotFound),
	)

	assert.False(t, indexes.IsReady(reader, "main", ref()))
	assert.True(t, indexes.IsReady(reader, "main", ref()))
	assert.False(t, indexes.IsReady(reader, "main", ref()))
}

func TestStatus_ReportsStoredOrUnspecified(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reader := NewMockLookup(ctrl)

	key := indexes.KeyFor("main", ref())

	gomock.InOrder(
		reader.EXPECT().Get(key).Return((&commonpb.Index{Id: ref(), BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY}).AsReader(), nil),
		reader.EXPECT().Get(key).Return(nil, domain.ErrNotFound),
	)

	assert.Equal(t, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY, indexes.Status(reader, "main", ref()))
	assert.Equal(t, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED, indexes.Status(reader, "main", ref()))
}

func TestPut_DispatchesPutIndex(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	writer := NewMockIndexWriter(ctrl)

	idx := &commonpb.Index{Id: ref(), Ledger: "main", BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING}
	writer.EXPECT().Put(indexes.KeyFor("main", ref()), idx)

	indexes.Put(writer, "main", idx)
}

func TestPut_NoOpOnNilIdx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	writer := NewMockIndexWriter(ctrl)
	// No EXPECT — Put must short-circuit before reaching PutIndex.

	indexes.Put(writer, "main", nil)
}

func TestPut_NoOpOnNilIDInsideIdx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	writer := NewMockIndexWriter(ctrl)

	indexes.Put(writer, "main", &commonpb.Index{Ledger: "main"})
}

func TestRemove_DispatchesDeleteIndex(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	writer := NewMockIndexWriter(ctrl)

	writer.EXPECT().Delete(indexes.KeyFor("main", ref()))

	indexes.Remove(writer, "main", ref())
}

func TestRemove_NoOpOnNilID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	writer := NewMockIndexWriter(ctrl)

	indexes.Remove(writer, "main", nil)
}

// TestKeyFor_BucketScopeUsesEmptyLedgerName pins the bucket-scope sentinel:
// passing an empty ledger name produces the IndexKey that carries the
// audit-style indexes (#436). The fixed-width LedgerName padding means the
// bucket-scope canonical bytes are 64 zero bytes followed by the IndexID
// canonical — distinct from any ledger-named slot.
func TestKeyFor_BucketScopeUsesEmptyLedgerName(t *testing.T) {
	t.Parallel()

	bucket := indexes.KeyFor("", ref())
	scoped := indexes.KeyFor("main", ref())

	require.Equal(t, "", bucket.LedgerName)
	require.Equal(t, "main", scoped.LedgerName)
	require.NotEqual(t, bucket.Bytes(), scoped.Bytes(), "bucket and ledger-scoped keys must not collide")
}
