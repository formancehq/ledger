package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/metadatalimits"
)

// testLimits is a small, easy-to-reason-about contract: the boundary cases below
// state their intent in numbers rather than leaning on the production defaults.
func testLimits() metadatalimits.MetadataLimits {
	return metadatalimits.MetadataLimits{
		MaxEntriesPerEntity:     3,
		MaxKeyBytes:             8,
		MaxValueBytes:           16,
		MaxTotalBytesPerEntity:  40,
		MaxTotalBytesPerCommand: 100,
	}
}

func TestMetadataValueSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value *commonpb.MetadataValue
		want  uint64
	}{
		{"nil value", nil, 0},
		{"empty type", &commonpb.MetadataValue{}, 0},
		{"empty string", commonpb.NewStringValue(""), 0},
		{"string counts bytes", commonpb.NewStringValue("abcde"), 5},
		{
			// Multi-byte runes count as bytes: the ceiling bounds what is
			// replicated and stored, which is bytes, not runes.
			name:  "multi-byte string counts bytes not runes",
			value: commonpb.NewStringValue("héllo"),
			want:  6,
		},
		{
			name:  "null value counts its original text",
			value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_NullValue{NullValue: &commonpb.NullValue{Original: "not-a-number"}}},
			want:  12,
		},
		{
			name:  "null value without original",
			value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_NullValue{}},
			want:  0,
		},
		{
			name:  "int is a fixed scalar width",
			value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_IntValue{IntValue: -1}},
			want:  metadatalimits.MetadataScalarValueBytes,
		},
		{
			name:  "uint is a fixed scalar width",
			value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_UintValue{UintValue: 1 << 62}},
			want:  metadatalimits.MetadataScalarValueBytes,
		},
		{
			name:  "datetime is a fixed scalar width",
			value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_DatetimeValue{DatetimeValue: 1_700_000_000_000_000}},
			want:  metadatalimits.MetadataScalarValueBytes,
		},
		{
			name:  "bool is one byte",
			value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_BoolValue{BoolValue: true}},
			want:  metadatalimits.MetadataBoolValueBytes,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, MetadataValueSize(tc.value))
		})
	}
}

// The entry size is the key plus the value, and a map's size is the sum of its
// entries — the property the per-entity and per-command ceilings are built on.
func TestMetadataEntryAndMapSize(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(3+5), MetadataEntrySize("abc", commonpb.NewStringValue("value")))

	m := map[string]*commonpb.MetadataValue{
		"a":  commonpb.NewStringValue("12345"),
		"bb": {Type: &commonpb.MetadataValue_BoolValue{BoolValue: true}},
	}
	require.Equal(t, uint64((1+5)+(2+1)), MetadataMapSize(m))

	require.Zero(t, MetadataMapSize(nil))
}

func TestMetadataLimitsConfigured(t *testing.T) {
	t.Parallel()

	require.True(t, DefaultMetadataLimits.Configured())
	require.True(t, testLimits().Configured())

	// A zero metadatalimits.MetadataLimits is the absence of configuration, never "unlimited".
	require.False(t, metadatalimits.MetadataLimits{}.Configured())

	// One missing ceiling is enough to make the contract unusable.
	for _, mutate := range []func(*metadatalimits.MetadataLimits){
		func(l *metadatalimits.MetadataLimits) { l.MaxEntriesPerEntity = 0 },
		func(l *metadatalimits.MetadataLimits) { l.MaxKeyBytes = 0 },
		func(l *metadatalimits.MetadataLimits) { l.MaxValueBytes = 0 },
		func(l *metadatalimits.MetadataLimits) { l.MaxTotalBytesPerEntity = 0 },
		func(l *metadatalimits.MetadataLimits) { l.MaxTotalBytesPerCommand = 0 },
	} {
		limits := testLimits()
		mutate(&limits)
		require.False(t, limits.Configured())
	}
}

func TestMetadataLimitsConsistent(t *testing.T) {
	t.Parallel()

	require.True(t, DefaultMetadataLimits.Consistent())

	// Equality is allowed: the ceilings may coincide.
	require.True(t, metadatalimits.MetadataLimits{
		MaxEntriesPerEntity:     1,
		MaxKeyBytes:             10,
		MaxValueBytes:           10,
		MaxTotalBytesPerEntity:  10,
		MaxTotalBytesPerCommand: 10,
	}.Consistent())

	tests := []struct {
		name   string
		mutate func(*metadatalimits.MetadataLimits)
	}{
		{"key above entity", func(l *metadatalimits.MetadataLimits) { l.MaxKeyBytes = l.MaxTotalBytesPerEntity + 1 }},
		{"value above entity", func(l *metadatalimits.MetadataLimits) { l.MaxValueBytes = l.MaxTotalBytesPerEntity + 1 }},
		{"entity above command", func(l *metadatalimits.MetadataLimits) { l.MaxTotalBytesPerEntity = l.MaxTotalBytesPerCommand + 1 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			limits := testLimits()
			tc.mutate(&limits)
			require.False(t, limits.Consistent())
		})
	}
}

func TestMetadataLimitsFromPolicy(t *testing.T) {
	t.Parallel()

	limits := MetadataLimitsFromPolicy(&commonpb.ClusterPolicy{
		MetadataMaxEntriesPerEntity: 1,
		MetadataMaxKeyBytes:         2,
		MetadataMaxValueBytes:       3,
		MetadataMaxEntityBytes:      4,
		MetadataMaxCommandBytes:     5,
	})
	require.Equal(t, metadatalimits.MetadataLimits{
		MaxEntriesPerEntity:     1,
		MaxKeyBytes:             2,
		MaxValueBytes:           3,
		MaxTotalBytesPerEntity:  4,
		MaxTotalBytesPerCommand: 5,
	}, limits)

	// A nil policy — or one carrying no ceilings — is unconfigured, so the
	// validators reject loudly instead of admitting unbounded metadata.
	require.False(t, MetadataLimitsFromPolicy(nil).Configured())
	require.False(t, MetadataLimitsFromPolicy(&commonpb.ClusterPolicy{Revision: 7}).Configured())
}

// Every ceiling accepts its exact boundary and rejects one unit past it, and the
// reported dimension identifies which ceiling was hit.
func TestMetadataLimitsValidateMapBoundaries(t *testing.T) {
	t.Parallel()

	limits := testLimits()

	tests := []struct {
		name          string
		metadata      map[string]*commonpb.MetadataValue
		wantDimension string
		wantLimit     uint64
		wantActual    uint64
	}{
		{
			name: "entry count at the ceiling",
			metadata: map[string]*commonpb.MetadataValue{
				"a": commonpb.NewStringValue(""),
				"b": commonpb.NewStringValue(""),
				"c": commonpb.NewStringValue(""),
			},
		},
		{
			name: "entry count one over",
			metadata: map[string]*commonpb.MetadataValue{
				"a": commonpb.NewStringValue(""),
				"b": commonpb.NewStringValue(""),
				"c": commonpb.NewStringValue(""),
				"d": commonpb.NewStringValue(""),
			},
			wantDimension: MetadataLimitDimensionEntries,
			wantLimit:     3,
			wantActual:    4,
		},
		{
			name:     "key at the ceiling",
			metadata: map[string]*commonpb.MetadataValue{strings.Repeat("k", 8): commonpb.NewStringValue("")},
		},
		{
			name:          "key one over",
			metadata:      map[string]*commonpb.MetadataValue{strings.Repeat("k", 9): commonpb.NewStringValue("")},
			wantDimension: MetadataLimitDimensionKey,
			wantLimit:     8,
			wantActual:    9,
		},
		{
			name:     "value at the ceiling",
			metadata: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue(strings.Repeat("v", 16))},
		},
		{
			name:          "value one over",
			metadata:      map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue(strings.Repeat("v", 17))},
			wantDimension: MetadataLimitDimensionValue,
			wantLimit:     16,
			wantActual:    17,
		},
		{
			name: "entity total at the ceiling",
			metadata: map[string]*commonpb.MetadataValue{
				"k1": commonpb.NewStringValue(strings.Repeat("v", 16)),
				"k2": commonpb.NewStringValue(strings.Repeat("v", 16)),
				"k3": commonpb.NewStringValue("vv"),
			},
		},
		{
			name: "entity total one over",
			metadata: map[string]*commonpb.MetadataValue{
				"k1": commonpb.NewStringValue(strings.Repeat("v", 16)),
				"k2": commonpb.NewStringValue(strings.Repeat("v", 16)),
				"k3": commonpb.NewStringValue("vvv"),
			},
			wantDimension: MetadataLimitDimensionEntity,
			wantLimit:     40,
			wantActual:    41,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateMetadataMap(limits, tc.metadata)

			if tc.wantDimension == "" {
				require.Nil(t, err, "the boundary itself must be accepted")

				return
			}

			require.NotNil(t, err)
			require.Equal(t, ErrReasonMetadataLimitExceeded, err.Reason())
			require.Equal(t, KindValidation, err.Kind())

			meta := FailureFactsOf(err).Facts
			require.Equal(t, tc.wantDimension, meta["dimension"])
			require.Equal(t, uint64ToString(tc.wantLimit), meta["limit"])
			require.Equal(t, uint64ToString(tc.wantActual), meta["actual"])
		})
	}
}

// An empty or nil map carries nothing and cannot violate any ceiling.
func TestMetadataLimitsValidateMapEmpty(t *testing.T) {
	t.Parallel()

	require.Nil(t, ValidateMetadataMap(testLimits(), nil))
	require.Nil(t, ValidateMetadataMap(testLimits(), map[string]*commonpb.MetadataValue{}))
}

// A per-entry failure names the offending key, so operator logs and the gRPC
// ErrorInfo identify it rather than only naming the rule.
func TestMetadataLimitsValidateMapNamesOffendingKey(t *testing.T) {
	t.Parallel()

	err := ValidateMetadataMap(testLimits(), map[string]*commonpb.MetadataValue{
		"ok":  commonpb.NewStringValue("fine"),
		"bad": commonpb.NewStringValue(strings.Repeat("v", 17)),
	})

	require.NotNil(t, err)
	require.Equal(t, "bad", FailureFactsOf(err).Facts["key"])
	require.Equal(t, MetadataLimitDimensionValue, FailureFactsOf(err).Facts["dimension"])
}

// The FSM validates merged metadata during apply, so the same input must produce
// the same rejection on every node. Go randomises map iteration order, so a map
// with several offending entries would otherwise report whichever one iteration
// reached first — a divergence hash-bound into the audit chain.
func TestMetadataLimitsValidateMapIsDeterministic(t *testing.T) {
	t.Parallel()

	metadata := map[string]*commonpb.MetadataValue{
		"zz-too-long-value": commonpb.NewStringValue(strings.Repeat("v", 17)),
		"aa-too-long-value": commonpb.NewStringValue(strings.Repeat("v", 17)),
		"mm-too-long-value": commonpb.NewStringValue(strings.Repeat("v", 17)),
	}

	limits := metadatalimits.MetadataLimits{
		MaxEntriesPerEntity:     10,
		MaxKeyBytes:             64,
		MaxValueBytes:           16,
		MaxTotalBytesPerEntity:  1 << 20,
		MaxTotalBytesPerCommand: 1 << 20,
	}

	first := ValidateMetadataMap(limits, metadata)
	require.NotNil(t, first)

	for range 100 {
		again := ValidateMetadataMap(limits, metadata)
		require.NotNil(t, again)
		require.Equal(t, first.Error(), again.Error())
		require.Equal(t, FailureFactsOf(first).Facts, FailureFactsOf(again).Facts)
	}

	// The lexicographically smallest offending key, not an arbitrary one.
	require.Equal(t, "aa-too-long-value", FailureFactsOf(first).Facts["key"])
}

// An entry breaking both the key and the value ceiling reports the key: the
// per-entry order is fixed so the choice never depends on iteration order.
func TestMetadataLimitsValidateEntryPrefersKeyOverValue(t *testing.T) {
	t.Parallel()

	err := ValidateMetadataMap(testLimits(), map[string]*commonpb.MetadataValue{
		strings.Repeat("k", 9): commonpb.NewStringValue(strings.Repeat("v", 17)),
	})

	require.NotNil(t, err)
	require.Equal(t, MetadataLimitDimensionKey, FailureFactsOf(err).Facts["dimension"])
	require.Equal(t, strings.Repeat("k", 9), FailureFactsOf(err).Facts["key"])
}

func TestMetadataLimitsValidateCommandBytes(t *testing.T) {
	t.Parallel()

	limits := testLimits()

	require.Nil(t, ValidateMetadataCommandBytes(limits, 0))
	require.Nil(t, ValidateMetadataCommandBytes(limits, 100), "the boundary itself must be accepted")

	err := ValidateMetadataCommandBytes(limits, 101)
	require.NotNil(t, err)
	require.Equal(t, MetadataLimitDimensionCommand, FailureFactsOf(err).Facts["dimension"])
	require.Equal(t, "100", FailureFactsOf(err).Facts["limit"])
	require.Equal(t, "101", FailureFactsOf(err).Facts["actual"])
	require.Equal(t, KindValidation, err.Kind())
}

func TestMetadataLimitsValidateKey(t *testing.T) {
	t.Parallel()

	limits := testLimits()

	require.Nil(t, ValidateMetadataLimitKey(limits, strings.Repeat("k", 8)))

	err := ValidateMetadataLimitKey(limits, strings.Repeat("k", 9))
	require.NotNil(t, err)
	require.Equal(t, MetadataLimitDimensionKey, FailureFactsOf(err).Facts["dimension"])
	require.Equal(t, strings.Repeat("k", 9), FailureFactsOf(err).Facts["key"])
}

// An unconfigured contract must reject rather than admit unbounded metadata:
// zero ceilings mean the policy carries no configuration, and failing open would
// silently remove the protection.
func TestMetadataLimitsUnconfiguredRejects(t *testing.T) {
	t.Parallel()

	var unconfigured metadatalimits.MetadataLimits

	for name, err := range map[string]Describable{
		"ValidateMap":          ValidateMetadataMap(unconfigured, map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}),
		"ValidateMap empty":    ValidateMetadataMap(unconfigured, nil),
		"ValidateCommandBytes": ValidateMetadataCommandBytes(unconfigured, 0),
		"ValidateKey":          ValidateMetadataLimitKey(unconfigured, "k"),
	} {
		require.NotNilf(t, err, "%s must reject an unconfigured contract", name)
		require.ErrorIsf(t, err, ErrMetadataLimitsUnconfigured, "%s", name)
		require.Equalf(t, ErrReasonClusterPolicyInvalid, err.Reason(), "%s", name)
	}
}

// uint64ToString keeps the boundary table readable without importing strconv
// into every case.
func uint64ToString(v uint64) string {
	return FailureFactsOf(&ErrMetadataLimitExceeded{Limit: v}).Facts["limit"]
}

// Public validation must preserve the exact persisted failures at Ledger's adapter.
func TestMetadataPublicFailureParity(t *testing.T) {
	t.Parallel()
	limits := testLimits()
	cases := []struct {
		metadata map[string]*commonpb.MetadataValue
		want     SerializableError
	}{
		{map[string]*commonpb.MetadataValue{"a": nil, "b": nil, "c": nil, "d": nil}, &ErrMetadataLimitExceeded{Dimension: MetadataLimitDimensionEntries, Limit: 3, Actual: 4}},
		{map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue(strings.Repeat("v", 40))}, &ErrMetadataLimitExceeded{Dimension: MetadataLimitDimensionEntity, Limit: 40, Actual: 41}},
		{map[string]*commonpb.MetadataValue{"long-key!": nil}, &ErrMetadataKeyValidation{Key: "long-key!", Cause: &ErrMetadataLimitExceeded{Dimension: MetadataLimitDimensionKey, Limit: 8, Actual: 9}}},
		{map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue(strings.Repeat("v", 17))}, &ErrMetadataKeyValidation{Key: "k", Cause: &ErrMetadataLimitExceeded{Dimension: MetadataLimitDimensionValue, Limit: 16, Actual: 17}}},
	}
	for _, tc := range cases {
		got := ValidateMetadataMap(limits, tc.metadata)
		require.Equal(t, tc.want.Error(), got.Error())
		require.Equal(t, tc.want.Kind(), got.Kind())
		require.Equal(t, tc.want.Reason(), got.Reason())
		wantBytes, err := tc.want.FailureDetails().MarshalVT()
		require.NoError(t, err)
		gotBytes, err := got.FailureDetails().MarshalVT()
		require.NoError(t, err)
		require.Equal(t, wantBytes, gotBytes)
	}
}
