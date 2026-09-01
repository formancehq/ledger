package plan

import (
	"errors"
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

// arbitrationFixture wires the minimum resolveCoverage needs to drive one
// index key down the miss path: an empty attribute cache (CheckCache →
// CacheMiss), a fresh loader, and an injected getValue whose first call is
// the primary load and whose second call is the stale-absence arbitration
// read.
func arbitrationFixture(t *testing.T) (*cache.Cache, *preload.AttributeLoader[*commonpb.Index], map[attributes.U128]CoverageEntry) {
	t.Helper()

	c, err := cache.New(1000, nil)
	require.NoError(t, err)

	canonical := []byte("bucket/ledger/meta:account:score")
	id, tag := attributes.MakeKey(canonical)

	return c, preload.NewAttributeLoader[*commonpb.Index](), map[attributes.U128]CoverageEntry{
		id: {Canonical: canonical, Tag: tag},
	}
}

// TestResolveCoverage_IndexArbitration_FreshReadFailureFailsResolve pins the
// error contract of the stale-absence arbitration: when the loader serves an
// absence for an index key and the arbitrating Pebble read fails, the resolve
// fails — the same contract as the primary load path. Degrading to a
// coverage-only plan instead would authorize the key with no seed, and apply
// would route back through the very cache the arbitration exists to distrust.
func TestResolveCoverage_IndexArbitration_FreshReadFailureFailsResolve(t *testing.T) {
	t.Parallel()

	c, loader, keys := arbitrationFixture(t)

	readErr := errors.New("injected pebble read failure")

	var calls atomic.Int32

	getValue := func(dal.PebbleGetter, []byte) (*commonpb.Index, error) {
		if calls.Add(1) == 1 {
			return nil, nil
		}

		return nil, readErr
	}

	_, err := resolveCoverage(
		keys, 1, 1, 1,
		c.Indexes, loader, getValue, nil,
		dal.SubAttrIndex, nil, nil, logging.Testing(), "indexes",
	)
	require.ErrorIs(t, err, readErr)
	require.EqualValues(t, 2, calls.Load(), "the absence must be arbitrated by a second read")
}

// TestResolveCoverage_IndexArbitration_FreshValueSeedsPlan pins the recovery
// path: a loader-served absence contradicted by the fresh read yields a
// seeded plan carrying the durable value.
func TestResolveCoverage_IndexArbitration_FreshValueSeedsPlan(t *testing.T) {
	t.Parallel()

	c, loader, keys := arbitrationFixture(t)

	var calls atomic.Int32

	getValue := func(dal.PebbleGetter, []byte) (*commonpb.Index, error) {
		if calls.Add(1) == 1 {
			return nil, nil
		}

		return &commonpb.Index{Ledger: "ledger"}, nil
	}

	res, err := resolveCoverage(
		keys, 1, 1, 1,
		c.Indexes, loader, getValue, nil,
		dal.SubAttrIndex, nil, nil, logging.Testing(), "indexes",
	)
	require.NoError(t, err)
	require.Len(t, res.attributes, 1)
	require.NotNil(t, res.attributes[0].GetValue(), "the arbitrated value must seed the plan")
}

// TestResolveCoverage_IndexKeyExemptFromBloomVeto pins the bloom-veto
// exemption for index registry keys: a filter that answers "definitely not
// present" must not short-circuit the Pebble load — the registry can hold a
// row the filter never learned, and a vetoed load would make every replica
// read an existing index as absent. The load runs and seeds the plan with
// the durable row.
func TestResolveCoverage_IndexKeyExemptFromBloomVeto(t *testing.T) {
	t.Parallel()

	c, loader, keys := arbitrationFixture(t)

	bfs := bloom.NewFilterSet(&commonpb.ClusterConfig{
		BloomIndexes: &commonpb.BloomTypeConfig{ExpectedKeys: 1024, FpRate: 0.001},
	}, nil)
	require.NotNil(t, bfs)

	filter := bfs.FilterForAttrType(dal.SubAttrIndex)
	require.NotNil(t, filter)

	var calls atomic.Int32

	getValue := func(dal.PebbleGetter, []byte) (*commonpb.Index, error) {
		calls.Add(1)

		return &commonpb.Index{Ledger: "ledger"}, nil
	}

	res, err := resolveCoverage(
		keys, 1, 1, 1,
		c.Indexes, loader, getValue, nil,
		dal.SubAttrIndex, nil, filter, logging.Testing(), "indexes",
	)
	require.NoError(t, err)
	require.NotZero(t, calls.Load(), "the bloom veto must not suppress the index registry load")
	require.Len(t, res.attributes, 1)
	require.NotNil(t, res.attributes[0].GetValue(), "the loaded row must seed the plan")
}

// TestResolveCoverage_IndexKeyCacheHitStillLoads pins the cache-hit
// fallthrough for index registry keys: the classifier answers hit for any
// resident under the key's id — tombstoned or tag-mismatched included —
// while the apply-path read treats those as absent, so a coverage-only
// shortcut would strand the durable row. Index keys load and seed even on a
// cache hit.
func TestResolveCoverage_IndexKeyCacheHitStillLoads(t *testing.T) {
	t.Parallel()

	c, loader, keys := arbitrationFixture(t)

	for id, entry := range keys {
		c.Indexes.Put(id, attributes.Entry[*commonpb.Index]{Tag: entry.Tag, Data: &commonpb.Index{Ledger: "stale"}})
	}

	var calls atomic.Int32

	getValue := func(dal.PebbleGetter, []byte) (*commonpb.Index, error) {
		calls.Add(1)

		return &commonpb.Index{Ledger: "ledger"}, nil
	}

	res, err := resolveCoverage(
		keys, 1, 1, 1,
		c.Indexes, loader, getValue, nil,
		dal.SubAttrIndex, nil, nil, logging.Testing(), "indexes",
	)
	require.NoError(t, err)
	require.NotZero(t, calls.Load(), "a cache hit must not skip the index registry load")
	require.Len(t, res.attributes, 1)
	require.NotNil(t, res.attributes[0].GetValue(), "the loaded row must seed the plan")
}
