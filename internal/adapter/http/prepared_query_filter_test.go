package http

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodePreparedQueryFilter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		raw       string
		wantErr   string
		assertion func(t *testing.T, filter any)
	}{
		{
			name:    "missing",
			raw:     "",
			wantErr: "filter is required",
		},
		{
			name:    "json null",
			raw:     "null",
			wantErr: "filter is required",
		},
		{
			name:    "empty object",
			raw:     "{}",
			wantErr: "must contain at least one condition",
		},
		{
			name:    "invalid json",
			raw:     "not-json",
			wantErr: "filter:",
		},
		{
			name: "and filter with nested field oneof",
			raw: `{"and":{"filters":[
				{"field":{"field":{"metadata":"x"},"intCond":{"min":"1"}}},
				{"field":{"field":{"metadata":"y"},"boolCond":{"hardcoded":true}}}
			]}}`,
		},
		{
			name: "or filter",
			raw:  `{"or":{"filters":[{"field":{"field":{"metadata":"x"},"existsCond":{}}}]}}`,
		},
		{
			name: "leaf field oneof",
			raw:  `{"field":{"field":{"metadata":"x"},"existsCond":{}}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			filter, err := decodePreparedQueryFilter(json.RawMessage(tc.raw))
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, filter)
			require.NotNil(t, filter.GetFilter(), "oneof discriminator must be populated")
		})
	}
}
