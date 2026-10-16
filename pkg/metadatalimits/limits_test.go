package metadatalimits_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/pkg/metadatalimits"
)

func stringBytes(s string) uint64 { return uint64(len(s)) }

func TestMetadataExternalContract(t *testing.T) {
	t.Parallel()
	l := metadatalimits.MetadataLimits{MaxEntriesPerEntity: 2, MaxKeyBytes: 4, MaxValueBytes: 5, MaxTotalBytesPerEntity: 12, MaxTotalBytesPerCommand: 20}
	require.NoError(t, l.Validate())
	require.Equal(t, uint64(7), metadatalimits.MetadataEntrySize("é", 5))
	require.Equal(t, uint64(12), metadatalimits.MetadataMapSize(map[string]string{"a": "12345", "b": "12345"}, stringBytes))
	cases := []struct {
		name          string
		m             map[string]string
		dimension     string
		key           string
		limit, actual uint64
	}{
		{name: "empty"},
		{name: "boundary", m: map[string]string{"a": "12345", "b": "12345"}},
		{name: "entries first", m: map[string]string{"a": "long value", "b": "", "c": ""}, dimension: "entries", limit: 2, actual: 3},
		{name: "entity before entry", m: map[string]string{"longkey": "123456"}, dimension: "entity", limit: 12, actual: 13},
		{name: "key before value", m: map[string]string{"abcde": "123456"}, dimension: "key", key: "abcde", limit: 4, actual: 5},
		{name: "value", m: map[string]string{"a": "123456"}, dimension: "value", key: "a", limit: 5, actual: 6},
		{name: "deterministic key", m: map[string]string{"zzzzz": "", "aaaaa": ""}, dimension: "key", key: "aaaaa", limit: 4, actual: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 30 {
				err := metadatalimits.ValidateMap(l, tc.m, stringBytes)
				if tc.dimension == "" {
					require.NoError(t, err)
					continue
				}
				failure, ok := errors.AsType[*metadatalimits.ErrMetadataLimitExceeded](err)
				require.True(t, ok)
				require.Equal(t, tc.dimension, failure.Dimension)
				require.Equal(t, tc.limit, failure.Limit)
				require.Equal(t, tc.actual, failure.Actual)
				keyed, ok := errors.AsType[*metadatalimits.ErrMetadataKeyValidation](err)
				require.Equal(t, tc.key != "", ok)
				if ok {
					require.Equal(t, tc.key, keyed.Key)
				}
			}
		})
	}
	require.NoError(t, l.ValidateEntry("k", 5))
	require.EqualError(t, l.ValidateEntry("k", 6), "metadata value size is 6 bytes, limit is 5")
	require.EqualError(t, l.ValidateEntry("12345", 6), "metadata key \"12345\" value: metadata key size is 5 bytes, limit is 4")
	require.NoError(t, l.ValidateEntry("k", 5))
	require.EqualError(t, l.ValidateEntry("k", 6), "metadata value size is 6 bytes, limit is 5")
	require.ErrorIs(t, (metadatalimits.MetadataLimits{}).ValidateEntry("k", 0), metadatalimits.ErrMetadataLimitsUnconfigured)
	require.NoError(t, l.ValidateKey("1234"))
	require.Error(t, l.ValidateKey("12345"))
	require.NoError(t, l.ValidateCommandBytes(20))
	require.EqualError(t, l.ValidateCommandBytes(21), "metadata command size is 21 bytes, limit is 20")
	require.ErrorIs(t, metadatalimits.ValidateMap(metadatalimits.MetadataLimits{}, map[string]string(nil), stringBytes), metadatalimits.ErrMetadataLimitsUnconfigured)
	require.ErrorIs(t, (metadatalimits.MetadataLimits{}).ValidateKey(""), metadatalimits.ErrMetadataLimitsUnconfigured)
	require.ErrorIs(t, (metadatalimits.MetadataLimits{}).ValidateCommandBytes(0), metadatalimits.ErrMetadataLimitsUnconfigured)
}

func TestMetadataPolicyValidation(t *testing.T) {
	t.Parallel()
	require.Error(t, (metadatalimits.MetadataLimits{}).Validate())
	for _, mutate := range []func(*metadatalimits.MetadataLimits){
		func(l *metadatalimits.MetadataLimits) { l.MaxKeyBytes = l.MaxTotalBytesPerEntity + 1 },
		func(l *metadatalimits.MetadataLimits) { l.MaxValueBytes = l.MaxTotalBytesPerEntity + 1 },
		func(l *metadatalimits.MetadataLimits) { l.MaxTotalBytesPerEntity = l.MaxTotalBytesPerCommand + 1 },
	} {
		l := metadatalimits.DefaultMetadataLimits
		mutate(&l)
		require.Error(t, l.Validate())
	}
	require.Equal(t, uint64(len("héllo")), metadatalimits.MetadataMapSize(map[string]string{"": "héllo"}, stringBytes))
	require.Equal(t, uint64(8), uint64(metadatalimits.MetadataScalarValueBytes))
	require.Equal(t, uint64(1), uint64(metadatalimits.MetadataBoolValueBytes))
	require.NoError(t, metadatalimits.ValidateMap(metadatalimits.DefaultMetadataLimits, map[string]string{"k": strings.Repeat("v", metadatalimits.DefaultMetadataMaxValueBytes)}, stringBytes))
}
