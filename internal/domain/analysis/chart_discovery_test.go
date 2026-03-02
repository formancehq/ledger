package analysis

import (
	"fmt"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeCompactAccount(address string) CompactAccount {
	return CompactAccount{Address: address}
}

func makeCompactAccountWithAssets(address string, assets ...string) CompactAccount {
	return CompactAccount{Address: address, Assets: assets}
}

func makeCompactAccountWithMetadata(address string, keys ...string) CompactAccount {
	return CompactAccount{Address: address, MetadataKeys: keys}
}

func TestAnalyze_EmptyAccounts(t *testing.T) {
	t.Parallel()

	resp := Analyze(nil, 0)
	require.NotNil(t, resp)
	require.NotNil(t, resp.SuggestedChart)
	assert.Empty(t, resp.SuggestedChart.Segments)
	assert.Empty(t, resp.Patterns)
	assert.Equal(t, uint64(0), resp.TotalAccounts)
}

func TestAnalyze_SingleAccount(t *testing.T) {
	t.Parallel()

	resp := Analyze([]CompactAccount{makeCompactAccount("world")}, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(1), resp.TotalAccounts)

	// Chart: single fixed segment "world"
	require.Len(t, resp.SuggestedChart.Segments, 1)
	assert.Equal(t, "world", resp.SuggestedChart.Segments[0].FixedValue)

	// Pattern: "world"
	require.Len(t, resp.Patterns, 1)
	assert.Equal(t, "world", resp.Patterns[0].Pattern)
	assert.Equal(t, uint64(1), resp.Patterns[0].AccountCount)
}

func TestAnalyze_SimpleFixedHierarchy(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccount("bank:main"),
		makeCompactAccount("bank:fees"),
		makeCompactAccount("world"),
	}

	resp := Analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(3), resp.TotalAccounts)

	// Chart should have 2 top-level segments: "bank" and "world"
	require.Len(t, resp.SuggestedChart.Segments, 2)

	// Find bank segment
	var bankSeg *commonpb.ChartSegment
	for _, s := range resp.SuggestedChart.Segments {
		if s.FixedValue == "bank" {
			bankSeg = s
		}
	}
	require.NotNil(t, bankSeg, "expected 'bank' segment")
	require.Len(t, bankSeg.Children, 2) // fees, main

	childValues := []string{bankSeg.Children[0].FixedValue, bankSeg.Children[1].FixedValue}
	assert.Contains(t, childValues, "main")
	assert.Contains(t, childValues, "fees")

	// Patterns should include "bank:main", "bank:fees", "world"
	assert.Len(t, resp.Patterns, 3)
}

func TestAnalyze_VariableDetection(t *testing.T) {
	t.Parallel()

	// Create 15 user accounts with UUIDs — exceeds default threshold of 10
	var accounts []CompactAccount
	for i := 0; i < 15; i++ {
		uuid := fmt.Sprintf("a0eebc99-9c0b-4ef8-bb6d-6bb9bd38%04x", i)
		accounts = append(accounts, makeCompactAccount("users:"+uuid))
	}

	resp := Analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(15), resp.TotalAccounts)

	// Chart: one top-level "users" segment with a variable child
	require.Len(t, resp.SuggestedChart.Segments, 1)
	usersSeg := resp.SuggestedChart.Segments[0]
	assert.Equal(t, "users", usersSeg.FixedValue)
	require.Len(t, usersSeg.Children, 1)
	assert.NotNil(t, usersSeg.Children[0].Variable)
	assert.Equal(t, "id", usersSeg.Children[0].Variable.Name)
	assert.Equal(t, uuidPattern, usersSeg.Children[0].Variable.InferredPattern)

	// Pattern: "users:{id}"
	require.Len(t, resp.Patterns, 1)
	assert.Equal(t, "users:{id}", resp.Patterns[0].Pattern)
	assert.Equal(t, uint64(15), resp.Patterns[0].AccountCount)

	// Segments should describe the variable
	require.Len(t, resp.Patterns[0].Segments, 2) // "users", then variable
	assert.Equal(t, servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE, resp.Patterns[0].Segments[1].Type)
}

func TestAnalyze_NumericPattern(t *testing.T) {
	t.Parallel()

	var accounts []CompactAccount
	for i := 0; i < 12; i++ {
		accounts = append(accounts, makeCompactAccount(fmt.Sprintf("orders:%d", 1000+i)))
	}

	resp := Analyze(accounts, 0)

	require.Len(t, resp.SuggestedChart.Segments, 1)
	seg := resp.SuggestedChart.Segments[0]
	assert.Equal(t, "orders", seg.FixedValue)
	require.Len(t, seg.Children, 1)
	require.NotNil(t, seg.Children[0].Variable)
	assert.Equal(t, "number", seg.Children[0].Variable.Name)
	assert.Equal(t, numericPattern, seg.Children[0].Variable.InferredPattern)
}

func TestAnalyze_DeepNestedHierarchy(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccount("platform:region:eu:main"),
		makeCompactAccount("platform:region:us:main"),
		makeCompactAccount("platform:region:eu:fees"),
	}

	resp := Analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(3), resp.TotalAccounts)

	// Should have a nested structure: platform -> region -> (eu, us) -> (main, fees)
	require.Len(t, resp.SuggestedChart.Segments, 1)
	assert.Equal(t, "platform", resp.SuggestedChart.Segments[0].FixedValue)
}

func TestAnalyze_AssetsAggregation(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccountWithAssets("bank:main", "EUR", "USD"),
		makeCompactAccountWithAssets("bank:fees", "USD"),
	}

	resp := Analyze(accounts, 0)

	// Find the "bank:main" pattern
	var mainPattern *servicepb.AccountPattern
	for _, p := range resp.Patterns {
		if p.Pattern == "bank:main" {
			mainPattern = p
		}
	}
	require.NotNil(t, mainPattern)
	assert.Equal(t, []string{"EUR", "USD"}, mainPattern.Assets)
}

func TestAnalyze_MetadataKeysAggregation(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccountWithMetadata("users:alice", "email", "role"),
		makeCompactAccountWithMetadata("users:bob", "phone", "role"),
	}

	resp := Analyze(accounts, 0)

	// With 2 users, they're fixed segments
	var alicePattern *servicepb.AccountPattern
	for _, p := range resp.Patterns {
		if p.Pattern == "users:alice" {
			alicePattern = p
		}
	}
	require.NotNil(t, alicePattern)
	assert.Contains(t, alicePattern.MetadataKeys, "role")
	assert.Contains(t, alicePattern.MetadataKeys, "email")
}

func TestAnalyze_ThresholdConfigurability(t *testing.T) {
	t.Parallel()

	// Create 5 user accounts with numeric IDs — below default threshold but above 3
	var accounts []CompactAccount
	for i := 0; i < 5; i++ {
		accounts = append(accounts, makeCompactAccount(fmt.Sprintf("users:%d", 1000+i)))
	}

	// With default threshold (10), should be fixed
	resp := Analyze(accounts, 0)
	require.Len(t, resp.SuggestedChart.Segments, 1)
	usersSeg := resp.SuggestedChart.Segments[0]
	require.Len(t, usersSeg.Children, 5)
	for _, child := range usersSeg.Children {
		assert.NotEmpty(t, child.FixedValue, "expected fixed children with default threshold")
	}

	// With threshold=3, should become variable (5 numeric IDs > threshold 3)
	resp2 := Analyze(accounts, 3)
	require.Len(t, resp2.SuggestedChart.Segments, 1)
	usersSeg2 := resp2.SuggestedChart.Segments[0]
	require.Len(t, usersSeg2.Children, 1)
	assert.NotNil(t, usersSeg2.Children[0].Variable, "expected variable child with threshold=3")
}

func TestAnalyze_WorldAndUsersPattern(t *testing.T) {
	t.Parallel()

	// Realistic pattern: world + bank + 15 users with UUIDs + wallet sub-accounts
	var accounts []CompactAccount
	accounts = append(accounts, makeCompactAccount("world"))
	accounts = append(accounts, makeCompactAccount("bank:main"))
	accounts = append(accounts, makeCompactAccount("bank:fees"))

	for i := 0; i < 15; i++ {
		uuid := fmt.Sprintf("a0eebc99-9c0b-4ef8-bb6d-6bb9bd38%04x", i)
		accounts = append(accounts, makeCompactAccount("users:"+uuid+":main"))
		accounts = append(accounts, makeCompactAccount("users:"+uuid+":savings"))
	}

	resp := Analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(33), resp.TotalAccounts)

	// Chart should have top-level: bank, users, world
	require.Len(t, resp.SuggestedChart.Segments, 3)
}

func TestInferPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{"empty", nil, ""},
		{"uuids", []string{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380000", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380001"}, uuidPattern},
		{"numeric", []string{"100", "200", "300"}, numericPattern},
		{"alphanum", []string{"hello", "world", "test-123"}, alphanumPattern},
		{"mixed", []string{"hello!", "wor ld"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, inferPattern(tt.values))
		})
	}
}

func TestInferVariableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{"empty", nil, "id"},
		{"uuids", []string{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380000"}, "id"},
		{"numeric", []string{"100", "200"}, "number"},
		{"other", []string{"hello", "world"}, "value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, inferVariableName(tt.values))
		})
	}
}
