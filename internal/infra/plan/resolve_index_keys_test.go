package plan

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Index registry keys take the same resolution path as every other attribute:
// a cache hit under the key's tag is coverage-only, a bloom-absent key is
// coverage-only, and only a genuine miss loads. These pin that no key class
// is special-cased.

func indexKeyFixture(t *testing.T) (*cache.Cache, *preload.AttributeLoader[*commonpb.Index], map[attributes.U128]CoverageEntry) {
	t.Helper()

	c, err := cache.New(1000, nil)
	require.NoError(t, err)

	canonical := []byte("bucket/ledger/meta:account:score")
	id, tag := attributes.MakeKey(canonical)

	return c, preload.NewAttributeLoader[*commonpb.Index](), map[attributes.U128]CoverageEntry{
		id: {Canonical: canonical, Tag: tag},
	}
}

func countingGetValue(t *testing.T, calls *atomic.Int32) func(dal.PebbleGetter, []byte) (*commonpb.Index, error) {
	t.Helper()

	return func(dal.PebbleGetter, []byte) (*commonpb.Index, error) {
		calls.Add(1)

		return &commonpb.Index{Ledger: "ledger"}, nil
	}
}

func TestResolveCoverage_IndexKeyCacheHitIsCoverageOnly(t *testing.T) {
	t.Parallel()

	c, loader, keys := indexKeyFixture(t)

	for id, entry := range keys {
		c.Indexes.Put(id, attributes.Entry[*commonpb.Index]{Tag: entry.Tag, Data: &commonpb.Index{Ledger: "ledger"}})
	}

	var calls atomic.Int32

	res, err := resolveCoverage(
		keys, 1, preload.CacheStamp{Boundary: 1, Epoch: 1},
		c.Indexes, loader, countingGetValue(t, &calls), nil,
		dal.SubAttrIndex, nil, nil, logging.Testing(), "indexes",
	)
	require.NoError(t, err)
	require.Zero(t, calls.Load(), "a cache hit needs no store read")
	require.Len(t, res.attributes, 1)
	require.Nil(t, res.attributes[0].GetValue(), "coverage-only: the apply reads the cached row")
}

// A resident under the same U128 but another canonical key's tag is not this
// key: the apply-path read rejects it, so admission must load and seed.
func TestResolveCoverage_ForeignTagResidentLoads(t *testing.T) {
	t.Parallel()

	c, loader, keys := indexKeyFixture(t)

	for id, entry := range keys {
		c.Indexes.Put(id, attributes.Entry[*commonpb.Index]{Tag: entry.Tag + 1, Data: &commonpb.Index{Ledger: "other"}})
	}

	var calls atomic.Int32

	res, err := resolveCoverage(
		keys, 1, preload.CacheStamp{Boundary: 1, Epoch: 1},
		c.Indexes, loader, countingGetValue(t, &calls), nil,
		dal.SubAttrIndex, nil, nil, logging.Testing(), "indexes",
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load(), "a colliding resident of another key is a miss")
	require.Len(t, res.attributes, 1)
	require.NotNil(t, res.attributes[0].GetValue(), "the loaded row seeds the plan")
}

func TestResolveCoverage_IndexKeyHonoursBloomVeto(t *testing.T) {
	t.Parallel()

	c, loader, keys := indexKeyFixture(t)

	bfs := bloom.NewFilterSet(&commonpb.ClusterConfig{
		BloomIndexes: &commonpb.BloomTypeConfig{ExpectedKeys: 1024, FpRate: 0.001},
	}, nil)
	require.NotNil(t, bfs)
	bfs.SetReady(true)

	filter := bfs.FilterForAttrType(dal.SubAttrIndex)
	require.NotNil(t, filter)

	var calls atomic.Int32

	res, err := resolveCoverage(
		keys, 1, preload.CacheStamp{Boundary: 1, Epoch: 1},
		c.Indexes, loader, countingGetValue(t, &calls), nil,
		dal.SubAttrIndex, nil, filter, logging.Testing(), "indexes",
	)
	require.NoError(t, err)
	require.Zero(t, calls.Load(), "a key the filter never learned is absent from the store")
	require.Len(t, res.attributes, 1)
	require.Nil(t, res.attributes[0].GetValue())
}

// A memoized absence is served only until the FSM releases the key: after a
// proposal covering it commits, the next preload loads from the store again.
func TestBuilder_ReleasePreloaded_DropsTheMemoizedLoad(t *testing.T) {
	t.Parallel()

	c, err := cache.New(1000, nil)
	require.NoError(t, err)

	b := NewBuilder(nil, c, attributes.New(), nil, nil, logging.Testing(), 0)

	canonical := []byte("bucket/ledger/meta:account:score")
	id, _ := attributes.MakeKey(canonical)
	stamp := preload.CacheStamp{Boundary: 1, Epoch: 1}

	first, err := b.loaders.Indexes.LoadOrWait(id, stamp, func() (*commonpb.Index, error) { return nil, nil })
	require.NoError(t, err)
	require.True(t, first.FromLoad)

	memo, err := b.loaders.Indexes.LoadOrWait(id, stamp, func() (*commonpb.Index, error) {
		t.Fatal("the memoized absence must be served before the release")

		return nil, nil
	})
	require.NoError(t, err)
	require.False(t, memo.FromLoad)

	b.ReleasePreloaded(dal.SubAttrIndex, id)

	reloaded, err := b.loaders.Indexes.LoadOrWait(id, stamp, func() (*commonpb.Index, error) {
		return &commonpb.Index{Ledger: "ledger"}, nil
	})
	require.NoError(t, err)
	require.True(t, reloaded.FromLoad, "the release must force a fresh load")
	require.NotNil(t, reloaded.Value)
}
