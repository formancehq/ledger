package analysis

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func analyze(accounts []CompactAccount, variableThreshold uint32) *servicepb.AnalyzeAccountsResponse {
	i := 0
	next := func() (CompactAccount, error) {
		if i >= len(accounts) {
			return CompactAccount{}, io.EOF
		}
		acc := accounts[i]
		i++

		return acc, nil
	}
	resp, err := AnalyzeFromIterator(next, variableThreshold, nil)
	if err != nil {
		panic(fmt.Sprintf("unexpected error from slice iterator: %v", err))
	}

	return resp
}

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

	resp := analyze(nil, 0)
	require.NotNil(t, resp)
	assert.Empty(t, resp.GetPatterns())
	assert.Equal(t, uint64(0), resp.GetTotalAccounts())
}

func TestAnalyze_SingleAccount(t *testing.T) {
	t.Parallel()

	resp := analyze([]CompactAccount{makeCompactAccount("world")}, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(1), resp.GetTotalAccounts())

	// Pattern: "world"
	require.Len(t, resp.GetPatterns(), 1)
	assert.Equal(t, "world", resp.GetPatterns()[0].GetPattern())
	assert.Equal(t, uint64(1), resp.GetPatterns()[0].GetAccountCount())
}

func TestAnalyze_SimpleFixedHierarchy(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccount("bank:main"),
		makeCompactAccount("bank:fees"),
		makeCompactAccount("world"),
	}

	resp := analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(3), resp.GetTotalAccounts())

	// Patterns should include "bank:main", "bank:fees", "world"
	assert.Len(t, resp.GetPatterns(), 3)
}

func TestAnalyze_VariableDetection(t *testing.T) {
	t.Parallel()

	// Create 15 user accounts with UUIDs — exceeds default threshold of 10
	var accounts []CompactAccount

	for i := range 15 {
		uuid := fmt.Sprintf("a0eebc99-9c0b-4ef8-bb6d-6bb9bd38%04x", i)
		accounts = append(accounts, makeCompactAccount("users:"+uuid))
	}

	resp := analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(15), resp.GetTotalAccounts())

	// Pattern: "users:{id}"
	require.Len(t, resp.GetPatterns(), 1)
	assert.Equal(t, "users:{id}", resp.GetPatterns()[0].GetPattern())
	assert.Equal(t, uint64(15), resp.GetPatterns()[0].GetAccountCount())

	// Segments should describe the variable
	require.Len(t, resp.GetPatterns()[0].GetSegments(), 2) // "users", then variable
	assert.Equal(t, servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE, resp.GetPatterns()[0].GetSegments()[1].GetType())
}

func TestAnalyze_NumericPattern(t *testing.T) {
	t.Parallel()

	var accounts []CompactAccount
	for i := range 12 {
		accounts = append(accounts, makeCompactAccount(fmt.Sprintf("orders:%d", 1000+i)))
	}

	resp := analyze(accounts, 0)

	require.Len(t, resp.GetPatterns(), 1)
	assert.Equal(t, "orders:{number}", resp.GetPatterns()[0].GetPattern())
}

func TestAnalyze_DeepNestedHierarchy(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccount("platform:region:eu:main"),
		makeCompactAccount("platform:region:us:main"),
		makeCompactAccount("platform:region:eu:fees"),
	}

	resp := analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(3), resp.GetTotalAccounts())
	assert.NotEmpty(t, resp.GetPatterns())
}

func TestAnalyze_AssetsAggregation(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccountWithAssets("bank:main", "EUR", "USD"),
		makeCompactAccountWithAssets("bank:fees", "USD"),
	}

	resp := analyze(accounts, 0)

	// Find the "bank:main" pattern
	var mainPattern *servicepb.AccountPattern

	for _, p := range resp.GetPatterns() {
		if p.GetPattern() == "bank:main" {
			mainPattern = p
		}
	}

	require.NotNil(t, mainPattern)
	assert.Equal(t, []string{"EUR", "USD"}, mainPattern.GetAssets())
}

func TestAnalyze_MetadataKeysAggregation(t *testing.T) {
	t.Parallel()

	accounts := []CompactAccount{
		makeCompactAccountWithMetadata("users:alice", "email", "role"),
		makeCompactAccountWithMetadata("users:bob", "phone", "role"),
	}

	resp := analyze(accounts, 0)

	// With 2 users, they're fixed segments
	var alicePattern *servicepb.AccountPattern

	for _, p := range resp.GetPatterns() {
		if p.GetPattern() == "users:alice" {
			alicePattern = p
		}
	}

	require.NotNil(t, alicePattern)
	assert.Contains(t, alicePattern.GetMetadataKeys(), "role")
	assert.Contains(t, alicePattern.GetMetadataKeys(), "email")
}

func TestAnalyze_ThresholdConfigurability(t *testing.T) {
	t.Parallel()

	// Create 5 user accounts with numeric IDs — below default threshold but above 3
	var accounts []CompactAccount
	for i := range 5 {
		accounts = append(accounts, makeCompactAccount(fmt.Sprintf("users:%d", 1000+i)))
	}

	// With default threshold (10), should be fixed — 5 patterns
	resp := analyze(accounts, 0)
	require.Len(t, resp.GetPatterns(), 5)

	// With threshold=3, should become variable (5 numeric IDs > threshold 3)
	resp2 := analyze(accounts, 3)
	require.Len(t, resp2.GetPatterns(), 1)
	assert.Equal(t, "users:{number}", resp2.GetPatterns()[0].GetPattern())
}

func TestAnalyze_WorldAndUsersPattern(t *testing.T) {
	t.Parallel()

	// Realistic pattern: world + bank + 15 users with UUIDs + wallet sub-accounts
	var accounts []CompactAccount

	accounts = append(accounts, makeCompactAccount("world"))
	accounts = append(accounts, makeCompactAccount("bank:main"))
	accounts = append(accounts, makeCompactAccount("bank:fees"))

	for i := range 15 {
		uuid := fmt.Sprintf("a0eebc99-9c0b-4ef8-bb6d-6bb9bd38%04x", i)
		accounts = append(accounts, makeCompactAccount("users:"+uuid+":main"))
		accounts = append(accounts, makeCompactAccount("users:"+uuid+":savings"))
	}

	resp := analyze(accounts, 0)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(33), resp.GetTotalAccounts())
	assert.NotEmpty(t, resp.GetPatterns())
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

func TestAnalyze_OverflowCapping(t *testing.T) {
	t.Parallel()

	// With threshold=5, childCap = 10. Create 100 UUIDs to force overflow.
	var accounts []CompactAccount
	for i := range 100 {
		uuid := fmt.Sprintf("a0eebc99-9c0b-4ef8-bb6d-6bb9bd38%04x", i)
		accounts = append(accounts, makeCompactAccount("users:"+uuid+":wallet"))
	}

	resp := analyze(accounts, 5)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(100), resp.GetTotalAccounts())

	// Pattern: "users:{id}:wallet"
	require.Len(t, resp.GetPatterns(), 1)
	assert.Equal(t, "users:{id}:wallet", resp.GetPatterns()[0].GetPattern())
	assert.Equal(t, uint64(100), resp.GetPatterns()[0].GetAccountCount())

	// UniqueValues should reflect the full count including overflow
	varSeg := resp.GetPatterns()[0].GetSegments()[1]
	assert.Equal(t, uint64(100), varSeg.GetUniqueValues())
}

func TestAnalyze_OverflowMemoryBounded(t *testing.T) {
	t.Parallel()

	// Verify that with many unique segments, the trie doesn't grow unbounded.
	// threshold=3, childCap=6. Create 1000 unique segments.
	const threshold uint32 = 3
	const numAccounts = 1000

	var accounts []CompactAccount
	for i := range numAccounts {
		accounts = append(accounts, makeCompactAccount(fmt.Sprintf("users:%06d", i)))
	}

	resp := analyze(accounts, threshold)

	require.NotNil(t, resp)
	assert.Equal(t, uint64(numAccounts), resp.GetTotalAccounts())

	require.Len(t, resp.GetPatterns(), 1)
	assert.Equal(t, "users:{number}", resp.GetPatterns()[0].GetPattern())
	assert.Equal(t, uint64(numAccounts), resp.GetPatterns()[0].GetAccountCount())
}
