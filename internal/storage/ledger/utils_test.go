package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/query"
)

func TestCanPushAddressFilterToLateral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		builder  query.Builder
		expected bool
	}{
		{
			name:     "nil builder",
			builder:  nil,
			expected: true,
		},
		{
			name:     "simple $match on address",
			builder:  query.Match("address", "users:"),
			expected: true,
		},
		{
			name:     "simple $match on account alias",
			builder:  query.Match("account", "users:"),
			expected: true,
		},
		{
			name:     "$and with address and metadata",
			builder:  query.And(query.Match("address", "users:"), query.Match("metadata[category]", "premium")),
			expected: true,
		},
		{
			name:     "$not with address - unsafe",
			builder:  query.Not(query.Match("address", "users:")),
			expected: false,
		},
		{
			name:     "$not with metadata only - safe",
			builder:  query.Not(query.Match("metadata[x]", "y")),
			expected: true,
		},
		{
			name:     "$or with two addresses - safe (all branches have address)",
			builder:  query.Or(query.Match("address", "users:"), query.Match("address", "world")),
			expected: true,
		},
		{
			name:     "$or single item - safe (no-op wrapper)",
			builder:  query.Or(query.Match("address", "users:")),
			expected: true,
		},
		{
			name:     "$or with address and balance - unsafe (mixed)",
			builder:  query.Or(query.Match("address", "users:"), query.Lte("balance[USD]", 0)),
			expected: false,
		},
		{
			name:     "$and with address and $not on metadata - safe (production case)",
			builder:  query.And(query.And(query.Or(query.Match("account", "xxx:")), query.Not(query.Match("metadata[wallet_transaction_method]", "hold_settlement")))),
			expected: true,
		},
		{
			name:     "$and with address and $not on address - unsafe",
			builder:  query.And(query.Match("address", "users:"), query.Not(query.Match("address", "world"))),
			expected: false,
		},
		{
			name:     "nested $not on metadata inside $and - safe",
			builder:  query.And(query.Match("address", "users:"), query.Not(query.Match("metadata[x]", "y"))),
			expected: true,
		},
		{
			name:     "nested $or on metadata inside $and - safe (no address in $or)",
			builder:  query.And(query.Match("address", "users:"), query.Or(query.Match("metadata[x]", "y"), query.Match("metadata[z]", "w"))),
			expected: true,
		},
		{
			name:     "$not wrapping $or with address - unsafe",
			builder:  query.Not(query.Or(query.Match("address", "a:"), query.Match("address", "b:"))),
			expected: false,
		},
		{
			name:     "$not wrapping $and with address - unsafe",
			builder:  query.Not(query.And(query.Match("address", "a:"), query.Match("metadata[x]", "y"))),
			expected: false,
		},
		{
			name:     "$not wrapping $or without address - safe",
			builder:  query.Not(query.Or(query.Match("metadata[x]", "y"), query.Match("metadata[z]", "w"))),
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := canPushAddressFilterToLateral(tc.builder)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildAddressFilterForLateral(t *testing.T) {
	t.Parallel()

	t.Run("single exact address", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"world"})
		assert.Equal(t, "address = 'world'", result)
	})

	t.Run("single partial address", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"users:"})
		assert.Contains(t, result, "address_array @@")
	})

	t.Run("single partial address with fixed segment", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"system:cashier:"})
		assert.Contains(t, result, `address_array @@ ('$[0] == "system"')::jsonpath`)
		assert.Contains(t, result, `address_array @@ ('$[1] == "cashier"')::jsonpath`)
	})

	t.Run("multiple addresses joined with OR", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"world", "users:"})
		assert.Contains(t, result, "(address = 'world')")
		assert.Contains(t, result, " OR ")
		assert.Contains(t, result, "address_array @@")
	})
}

func TestCollectAddressFilters(t *testing.T) {
	t.Parallel()

	t.Run("no address filter", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{}}
		addresses, needSegments := collectAddressFilters(mock)
		assert.Empty(t, addresses)
		assert.False(t, needSegments)
	})

	t.Run("single exact address", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{
			"address": {"world"},
		}}
		addresses, needSegments := collectAddressFilters(mock)
		require.Len(t, addresses, 1)
		assert.Equal(t, "world", addresses[0])
		assert.False(t, needSegments)
	})

	t.Run("single partial address", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{
			"address": {"users:"},
		}}
		addresses, needSegments := collectAddressFilters(mock)
		require.Len(t, addresses, 1)
		assert.Equal(t, "users:", addresses[0])
		assert.True(t, needSegments)
	})

	t.Run("mixed exact and partial addresses", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{
			"address": {"world", "users:"},
		}}
		addresses, needSegments := collectAddressFilters(mock)
		require.Len(t, addresses, 2)
		assert.Equal(t, "world", addresses[0])
		assert.Equal(t, "users:", addresses[1])
		assert.True(t, needSegments)
	})
}

// mockUseFilter implements the interface expected by collectAddressFilters.
// It simulates RepositoryHandlerBuildContext.UseFilter behavior:
// iterates all values for the given key and calls each matcher.
type mockUseFilter struct {
	filters map[string][]any
}

func (m *mockUseFilter) UseFilter(key string, matchers ...func(any) bool) bool {
	values, ok := m.filters[key]
	if !ok {
		return false
	}
	if len(matchers) == 0 {
		return true
	}
	for _, value := range values {
		allMatch := true
		for _, matcher := range matchers {
			if !matcher(value) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return true
		}
	}
	return false
}
