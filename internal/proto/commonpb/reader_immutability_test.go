package commonpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// These tests pin the immutability contract of protoc-gen-reader for maps,
// repeated message slices, and singular bytes fields.

func TestTransactionReader_GetPostings_ListReaderProtectsElements(t *testing.T) {
	t.Parallel()

	posting := &commonpb.Posting{Source: "world", Destination: "user:1", Asset: "USD"}
	tx := &commonpb.Transaction{Postings: []*commonpb.Posting{posting}}
	r := tx.AsReader()

	list := r.GetPostings()
	require.Equal(t, 1, list.Len())

	read := list.Get(0)
	require.Equal(t, "world", read.GetSource())

	// Mutate goes through a deep clone — original untouched.
	clone := read.Mutate()
	clone.Source = "MUTATED"
	clone.Destination = "ALSO_MUTATED"

	require.Equal(t, "world", posting.GetSource())
	require.Equal(t, "user:1", posting.GetDestination())
}

func TestTransactionReader_GetMetadata_MapReaderProtectsValues(t *testing.T) {
	t.Parallel()

	val := &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "original"}}
	tx := &commonpb.Transaction{Metadata: map[string]*commonpb.MetadataValue{"key": val}}
	r := tx.AsReader()

	m := r.GetMetadata()
	require.Equal(t, 1, m.Len())

	got, ok := m.Get("key")
	require.True(t, ok)
	require.NotNil(t, got)

	missing, ok := m.Get("nope")
	require.False(t, ok)
	require.Nil(t, missing)

	// Mutate returns a deep clone — overwriting it must not touch the original.
	clone := got.Mutate()
	clone.Type = &commonpb.MetadataValue_StringValue{StringValue: "MUTATED"}

	originalStr, originalOK := val.GetType().(*commonpb.MetadataValue_StringValue)
	require.True(t, originalOK)
	require.Equal(t, "original", originalStr.StringValue)
}

func TestTransactionReader_GetMetadata_RangeYieldsReaderViews(t *testing.T) {
	t.Parallel()

	tx := &commonpb.Transaction{
		Metadata: map[string]*commonpb.MetadataValue{
			"a": {Type: &commonpb.MetadataValue_StringValue{StringValue: "A"}},
			"b": {Type: &commonpb.MetadataValue_StringValue{StringValue: "B"}},
		},
	}
	r := tx.AsReader()

	seen := map[string]bool{}
	r.GetMetadata().Range(func(k string, v commonpb.MetadataValueReader) bool {
		seen[k] = v != nil

		return true
	})

	require.Equal(t, map[string]bool{"a": true, "b": true}, seen)
}

func TestScriptReader_GetContentHash_ReturnsIndependentBytes(t *testing.T) {
	t.Parallel()

	script := &commonpb.Script{Plain: "send", ContentHash: []byte{0xCA, 0xFE, 0xBA, 0xBE}}
	r := script.AsReader()

	got := r.GetContentHash()
	got[0] = 0x00
	got[1] = 0x00

	require.Equal(t, []byte{0xCA, 0xFE, 0xBA, 0xBE}, script.GetContentHash())
}

func TestScriptReader_GetVars_MapReaderLookupAndRange(t *testing.T) {
	t.Parallel()

	script := &commonpb.Script{Vars: map[string]string{"amount": "100", "ccy": "USD"}}
	r := script.AsReader()

	m := r.GetVars()
	require.Equal(t, 2, m.Len())

	amount, ok := m.Get("amount")
	require.True(t, ok)
	require.Equal(t, "100", amount)

	missing, ok := m.Get("missing")
	require.False(t, ok)
	require.Equal(t, "", missing)

	seen := map[string]string{}
	m.Range(func(k, v string) bool {
		seen[k] = v

		return true
	})
	require.Equal(t, map[string]string{"amount": "100", "ccy": "USD"}, seen)
}

func TestTransactionListReader_NilElementsTolerated(t *testing.T) {
	t.Parallel()

	list := commonpb.NewTransactionListReader([]*commonpb.Transaction{
		{Id: 1},
		nil,
		{Id: 3},
	})

	require.Equal(t, 3, list.Len())
	require.NotNil(t, list.Get(0))
	require.Nil(t, list.Get(1))
	require.NotNil(t, list.Get(2))

	var ids []uint64
	list.Range(func(_ int, r commonpb.TransactionReader) bool {
		if r == nil {
			ids = append(ids, 0)

			return true
		}
		ids = append(ids, r.GetId())

		return true
	})
	require.Equal(t, []uint64{1, 0, 3}, ids)
}
