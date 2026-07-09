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
			wantErr: "exactly one of",
		},
		{
			name:    "invalid json",
			raw:     "not-json",
			wantErr: "filter:",
		},
		{
			name: "and filter with nested field conditions",
			raw: `{"and":[
				{"match":{"type":"field","metadata":"x","condition":{"type":"int","min":1}}},
				{"match":{"type":"field","metadata":"y","condition":{"type":"bool","equals":true}}}
			]}`,
		},
		{
			name: "or filter",
			raw:  `{"or":[{"match":{"type":"field","metadata":"x","condition":{"type":"exists"}}}]}`,
		},
		{
			name: "leaf field condition",
			raw:  `{"match":{"type":"field","metadata":"x","condition":{"type":"exists"}}}`,
		},
		{
			name:    "unknown condition type rejected",
			raw:     `{"match":{"type":"bogus"}}`,
			wantErr: "unknown condition type",
		},
		{
			name:    "multiple top-level keys rejected",
			raw:     `{"and":[],"not":{"match":{"type":"reverted","value":true}}}`,
			wantErr: "exactly one of",
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
