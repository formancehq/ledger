package plan

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type countingPebbleGetter struct{ calls atomic.Int64 }

type testCloser struct{}

func (testCloser) Close() error { return nil }

func (g *countingPebbleGetter) Get([]byte) ([]byte, io.Closer, error) {
	g.calls.Add(1)

	return nil, testCloser{}, nil
}

func TestResolveCoverage_CapturesGen1WithoutPebbleRead(t *testing.T) {
	t.Parallel()

	c, err := cache.New(10, nil)
	require.NoError(t, err)
	c.SetCurrentGeneration(0)

	canonical := []byte("ledger/metadata")
	id, tag := attributes.MakeKey(canonical)
	cached := commonpb.NewStringValue("from-gen1")
	c.AccountMetadata.Gen1().Put(id, attributes.Entry[*commonpb.MetadataValue]{Tag: tag, Data: cached})

	getter := &countingPebbleGetter{}
	result, err := resolveCoverage[*commonpb.MetadataValue](
		map[attributes.U128]CoverageEntry{id: {Canonical: canonical, Tag: tag}},
		15, 11, 1,
		c.AccountMetadata,
		preload.NewAttributeLoader[*commonpb.MetadataValue](),
		func(dal.PebbleGetter, []byte) (*commonpb.MetadataValue, error) {
			getter.calls.Add(1)

			return nil, nil
		},
		getter,
		dal.SubAttrMetadata,
		nil,
		nil,
		logging.FromContext(logging.TestingContext()),
		"account_metadata",
	)
	require.NoError(t, err)
	require.Equal(t, int64(0), getter.calls.Load(), "Gen1 capture must avoid Pebble")
	require.Len(t, result.attributes, 1)
	decoded := &commonpb.MetadataValue{}
	require.NoError(t, decoded.UnmarshalVT(result.attributes[0].GetValue().GetRawValue()))
	require.Equal(t, cached, decoded)
}

func TestResolveCoverage_Gen1TombstoneUsesCoverageOnly(t *testing.T) {
	t.Parallel()

	c, err := cache.New(10, nil)
	require.NoError(t, err)
	c.SetCurrentGeneration(0)

	canonical := []byte("ledger/deleted")
	id, tag := attributes.MakeKey(canonical)
	c.AccountMetadata.Gen1().Put(id, attributes.Entry[*commonpb.MetadataValue]{Tag: tag, Deleted: true})
	getter := &countingPebbleGetter{}

	result, err := resolveCoverage[*commonpb.MetadataValue](
		map[attributes.U128]CoverageEntry{id: {Canonical: canonical, Tag: tag}},
		15, 11, 1,
		c.AccountMetadata,
		preload.NewAttributeLoader[*commonpb.MetadataValue](),
		func(dal.PebbleGetter, []byte) (*commonpb.MetadataValue, error) {
			getter.calls.Add(1)

			return nil, nil
		},
		getter,
		dal.SubAttrMetadata,
		nil,
		nil,
		logging.FromContext(logging.TestingContext()),
		"account_metadata",
	)
	require.NoError(t, err)
	require.Equal(t, int64(0), getter.calls.Load())
	require.Len(t, result.attributes, 1)
	require.Nil(t, result.attributes[0].GetValue())
}

// TestBuildPreloadPayload_RoundTrip pins the wire contract of the generic
// builder: marshal a typed proto into Preload.raw_value, unmarshal it back
// into a fresh instance of the same type, and assert equality on a field
// the test caller controls.
func TestBuildPreloadPayload_RoundTrip(t *testing.T) {
	t.Parallel()

	source := &commonpb.TransactionReferenceValue{TransactionId: 123}

	attrValue, err := buildPreloadPayload(dal.SubAttrReference, source)
	require.NoError(t, err)

	got := &commonpb.TransactionReferenceValue{}
	require.NoError(t, got.UnmarshalVT(attrValue.GetRawValue()))
	require.Equal(t, source.GetTransactionId(), got.GetTransactionId())
}

// TestBuildPreloadPayload_NilValue confirms the builder accepts a nil-zero
// pointer — vtproto marshals a typed nil to empty bytes, the FSM-side
// unmarshal restores a fresh empty proto. After EN-1378 every attribute
// (Volume included) follows this default and Declare-on-absent is the
// uniform pattern; no attribute injects a typed zero into the plan anymore.
func TestBuildPreloadPayload_NilValue(t *testing.T) {
	t.Parallel()

	var nilRef *commonpb.TransactionReferenceValue

	attrValue, err := buildPreloadPayload(dal.SubAttrReference, nilRef)
	require.NoError(t, err)
	require.Empty(t, attrValue.GetRawValue())

	got := &commonpb.TransactionReferenceValue{}
	require.NoError(t, got.UnmarshalVT(attrValue.GetRawValue()))
	require.Equal(t, uint64(0), got.GetTransactionId())
}
