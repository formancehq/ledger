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
			wantErr: "empty object",
		},
		{
			name:    "invalid json",
			raw:     "not-json",
			wantErr: "filter:",
		},
		{
			name: "and filter with nested conditions",
			raw: `{"$and":[
				{"$gte":{"metadata[x]":1}},
				{"$match":{"metadata[y]":true}}
			]}`,
		},
		{
			name: "or filter",
			raw:  `{"$or":[{"$exists":{"metadata":"x"}}]}`,
		},
		{
			name: "leaf metadata exists",
			raw:  `{"$exists":{"metadata":"x"}}`,
		},
		{
			name:    "unknown operator rejected",
			raw:     `{"$bogus":{}}`,
			wantErr: "unknown operator",
		},
		// Per-target validity (rejecting a condition invalid on the query's
		// specific target, e.g. logId/date/ledger on an ACCOUNTS query) is now
		// enforced by domain.ValidateFilterForTarget at the admission/FSM layers
		// and by the create handler — see TestValidateFilterForTarget
		// (internal/domain) and the admission validate_order tests. This decoder
		// is purely structural, so a log-only condition decodes fine here.
		{
			name: "log-only condition decodes (target validity checked later)",
			raw:  `{"$gt":{"logId":"5"}}`,
		},
		{
			name:    "null value rejected",
			raw:     `{"$match":{"reference":null}}`,
			wantErr: "must not be null",
		},
		{
			name:    "multiple top-level operators rejected",
			raw:     `{"$and":[],"$not":{"$match":{"reverted":true}}}`,
			wantErr: "exactly one operator",
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
