package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWantsBigintAsString(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		headerSet   bool
		headerValue string
		want        bool
	}

	testCases := []testCase{
		{name: "header absent", headerSet: false, want: false},
		{name: "empty value", headerSet: true, headerValue: "", want: false},
		{name: "true", headerSet: true, headerValue: "true", want: true},
		{name: "True", headerSet: true, headerValue: "True", want: true},
		{name: "TRUE", headerSet: true, headerValue: "TRUE", want: true},
		{name: "yes", headerSet: true, headerValue: "yes", want: true},
		{name: "y", headerSet: true, headerValue: "y", want: true},
		{name: "one", headerSet: true, headerValue: "1", want: true},
		{name: "padded", headerSet: true, headerValue: "  true  ", want: true},
		{name: "false", headerSet: true, headerValue: "false", want: false},
		{name: "zero", headerSet: true, headerValue: "0", want: false},
		{name: "unparseable is treated as absent", headerSet: true, headerValue: "maybe", want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.headerSet {
				r.Header.Set(httpHeaderBigintAsString, tc.headerValue)
			}

			require.Equal(t, tc.want, wantsBigintAsString(r))
		})
	}
}
