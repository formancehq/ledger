package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldFenceTransactionsDataset(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		filters map[string][]any
		expect  bool
	}{
		{
			name:    "no filter",
			filters: map[string][]any{},
			expect:  false,
		},
		{
			name:    "account needle",
			filters: map[string][]any{"account": {"users:alice"}},
			expect:  true,
		},
		{
			name:    "source needle",
			filters: map[string][]any{"source": {"world"}},
			expect:  true,
		},
		{
			name:    "destination needle",
			filters: map[string][]any{"destination": {"bank"}},
			expect:  true,
		},
		{
			name:    "metadata needle",
			filters: map[string][]any{"metadata": {"wallet_id"}},
			expect:  true,
		},
		{
			name:    "id only is not a needle",
			filters: map[string][]any{"id": {uint64(42)}},
			expect:  false,
		},
		{
			name:    "timestamp only is not a needle",
			filters: map[string][]any{"timestamp": {"2026-06-11T00:00:00Z"}},
			expect:  false,
		},
		{
			name:    "reference only is not a needle",
			filters: map[string][]any{"reference": {"tx1"}},
			expect:  false,
		},
		{
			name:    "reverted only is not a needle",
			filters: map[string][]any{"reverted": {true}},
			expect:  false,
		},
		{
			name: "needle combined with range still fences",
			filters: map[string][]any{
				"account":   {"users:alice"},
				"timestamp": {"2026-06-11T00:00:00Z"},
			},
			expect: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mock := &mockUseFilter{filters: tc.filters}
			assert.Equal(t, tc.expect, shouldFenceTransactionsDataset(mock))
		})
	}
}
