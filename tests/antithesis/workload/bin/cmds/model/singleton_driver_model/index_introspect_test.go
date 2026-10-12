package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func servedSet(canonicals ...string) map[string]bool {
	out := make(map[string]bool, len(canonicals))
	for _, c := range canonicals {
		out[c] = true
	}

	return out
}

// metaIdxCanonical is the registry key of an account metadata index on key.
func metaIdxCanonical(key string) string {
	return indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))
}

func TestIndexSetEqual(t *testing.T) {
	t.Parallel()

	// A declared field plus its index, so the registry holds exactly one entry.
	ls := buildLedger(t,
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1")),
	)

	one := metaIdxCanonical("k1")

	require.True(t, indexSetEqual(ls, servedSet(one)))

	// A lost row and an invented row are both mismatches.
	require.False(t, indexSetEqual(ls, servedSet()))
	require.False(t, indexSetEqual(ls, servedSet(one, metaIdxCanonical("k2"))))
	require.False(t, indexSetEqual(ls, servedSet(metaIdxCanonical("k2"))))
}

func TestIndexSetEqual_EmptyRegistry(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t, oracletest.AddTypeReq("acc"))

	require.True(t, indexSetEqual(ls, servedSet()))
	require.False(t, indexSetEqual(ls, servedSet(metaIdxCanonical("k1"))))
}

// The removal cascade is the case an exact set check has to get right: dropping
// the field declaration takes its registry entry with it, so a listing that still
// carries the index is a finding rather than lag.
func TestIndexSetEqual_FieldRemovalCascade(t *testing.T) {
	t.Parallel()

	declared := buildLedger(t,
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1")),
	)
	require.True(t, indexSetEqual(declared, servedSet(metaIdxCanonical("k1"))))

	removed := buildLedger(t,
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1")),
		oracletest.RemoveFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1"),
	)

	require.True(t, indexSetEqual(removed, servedSet()))
	require.False(t, indexSetEqual(removed, servedSet(metaIdxCanonical("k1"))))
}

// IndexState is what the presence check reads, so pin that it agrees with the
// set view on both a declared and an undeclared canonical.
func TestIndexStatePresence(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1")),
	)

	exists, _ := ls.IndexState(metaIdxCanonical("k1"))
	require.True(t, exists)

	exists, _ = ls.IndexState(metaIdxCanonical("k2"))
	require.False(t, exists)
}

func TestJoinSortedKeys(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", joinSortedKeys(servedSet()))
	require.Equal(t, "a,b,c", joinSortedKeys(servedSet("c", "a", "b")))
}
