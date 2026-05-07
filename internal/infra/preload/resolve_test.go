package preload

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestBuildIdempotencyKeyPreload_WithValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id:  []byte{1, 2, 3},
		Tag: 42,
	}

	value := &commonpb.IdempotencyKeyValue{
		LogSequence: 5,
		Hash:        []byte("test-hash"),
	}

	preload := buildIdempotencyKeyPreload(id, value)
	require.NotNil(t, preload)

	ik := preload.GetIdempotencyKey()
	require.NotNil(t, ik)
	assert.Equal(t, id, ik.GetId())
	assert.Equal(t, uint64(5), ik.GetValue().GetLogSequence())
	assert.Equal(t, []byte("test-hash"), ik.GetValue().GetHash())
}

func TestBuildIdempotencyKeyPreload_NilValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{1, 2, 3},
	}

	preload := buildIdempotencyKeyPreload(id, nil)
	require.NotNil(t, preload)

	ik := preload.GetIdempotencyKey()
	require.NotNil(t, ik)
	assert.Equal(t, uint64(0), ik.GetValue().GetLogSequence())
	assert.Nil(t, ik.GetValue().GetHash())
}

func TestBuildReferencePreload_WithValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id:  []byte{4, 5, 6},
		Tag: 7,
	}

	value := &commonpb.TransactionReferenceValue{
		TransactionId: 123,
	}

	preload := buildReferencePreload(id, value)
	require.NotNil(t, preload)

	ref := preload.GetTransactionReference()
	require.NotNil(t, ref)
	assert.Equal(t, id, ref.GetId())
	assert.Equal(t, uint64(123), ref.GetValue().GetTransactionId())
}

func TestBuildReferencePreload_NilValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{4, 5, 6},
	}

	preload := buildReferencePreload(id, nil)
	require.NotNil(t, preload)

	ref := preload.GetTransactionReference()
	require.NotNil(t, ref)
	assert.Equal(t, uint64(0), ref.GetValue().GetTransactionId())
}

func TestBuildSinkConfigPreload_WithValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id:  []byte{7, 8, 9},
		Tag: 10,
	}

	config := &commonpb.SinkConfig{
		Name: "test-sink",
	}

	preload := buildSinkConfigPreload(id, config)
	require.NotNil(t, preload)

	sc := preload.GetSinkConfig()
	require.NotNil(t, sc)
	assert.Equal(t, id, sc.GetId())
	assert.Equal(t, config, sc.GetValue())
}

func TestBuildSinkConfigPreload_NilValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{7, 8, 9},
	}

	preload := buildSinkConfigPreload(id, nil)
	require.NotNil(t, preload)

	sc := preload.GetSinkConfig()
	require.NotNil(t, sc)
	assert.Nil(t, sc.GetValue())
}

func TestBuildMetadataPreload_WithValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id:  []byte{10, 11, 12},
		Tag: 13,
	}

	value := &commonpb.MetadataValue{
		Type: &commonpb.MetadataValue_StringValue{StringValue: "test-value"},
	}

	preload := buildMetadataPreload(id, value)
	require.NotNil(t, preload)

	md := preload.GetAccountMetadata()
	require.NotNil(t, md)
	assert.Equal(t, id, md.GetId())
	assert.Equal(t, value, md.GetValue())
}

func TestBuildMetadataPreload_NilValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{10, 11, 12},
	}

	preload := buildMetadataPreload(id, nil)
	require.NotNil(t, preload)

	md := preload.GetAccountMetadata()
	require.NotNil(t, md)
	assert.Nil(t, md.GetValue())
}

func TestBuildVolumePreload_WithValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{1, 2},
	}

	vol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
	}

	preload := buildVolumePreload(id, vol)
	require.NotNil(t, preload)

	v := preload.GetVolume()
	require.NotNil(t, v)
	assert.Equal(t, id, v.GetId())
	assert.Equal(t, commonpb.NewUint256FromUint64(100), v.GetValue().GetInput())
	assert.Equal(t, commonpb.NewUint256FromUint64(50), v.GetValue().GetOutput())
}

func TestBuildVolumePreload_NilValue(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{1, 2},
	}

	preload := buildVolumePreload(id, nil)
	require.NotNil(t, preload)

	v := preload.GetVolume()
	require.NotNil(t, v)
	// Nil volumes should default to zero
	assert.Equal(t, commonpb.NewUint256FromUint64(0), v.GetValue().GetInput())
	assert.Equal(t, commonpb.NewUint256FromUint64(0), v.GetValue().GetOutput())
}

func TestBuildLedgerPreload(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id:  []byte{1, 2, 3},
		Tag: 5,
	}

	info := &commonpb.LedgerInfo{
		Name: "test-ledger",
	}

	preload := buildLedgerPreload(id, info)
	require.NotNil(t, preload)

	l := preload.GetLedger()
	require.NotNil(t, l)
	assert.Equal(t, id, l.GetId())
	assert.Equal(t, info, l.GetValue())
}

func TestBuildBoundaryPreload(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{3, 4},
	}

	boundaries := &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
	}

	preload := buildBoundaryPreload(id, boundaries)
	require.NotNil(t, preload)

	b := preload.GetBoundary()
	require.NotNil(t, b)
	assert.Equal(t, id, b.GetId())
	assert.Equal(t, boundaries, b.GetValue())
}

func TestBuildNumscriptVersionPreload(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{5, 6},
	}

	preload := buildNumscriptVersionPreload(id, &commonpb.NumscriptVersionValue{Version: "v1.2.3"})
	require.NotNil(t, preload)

	nv := preload.GetNumscriptVersion()
	require.NotNil(t, nv)
	assert.Equal(t, id, nv.GetId())
	assert.Equal(t, "v1.2.3", nv.GetValue().GetVersion())
}

func TestBuildTransactionStatePreload(t *testing.T) {
	t.Parallel()

	id := &raftcmdpb.AttributeID{
		Id: []byte{11, 12},
	}

	state := &commonpb.TransactionState{
		RevertedByTransaction: 42,
	}

	preload := buildTransactionStatePreload(id, state)
	require.NotNil(t, preload)

	ts := preload.GetTransactionState()
	require.NotNil(t, ts)
	assert.Equal(t, id, ts.GetId())
	assert.Equal(t, state, ts.GetValue())
}
