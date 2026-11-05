package components_test

import (
	"encoding/json"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQueryParamsPreserveExistingPublicVariants(t *testing.T) {
	size := int64(15)
	expand := "volumes"
	cursor := "cursor"
	group := int64(2)
	insertion := true
	fixtures := []struct {
		value    components.V2QueryParams
		expected string
	}{
		{components.CreateV2QueryParamsQueryTemplateAccountParams(components.QueryTemplateAccountParams{Resource: components.V2QueryParams1ResourceAccounts, PageSize: &size, Expand: &expand, Cursor: &cursor}), `{"resource":"accounts","pageSize":15,"expand":"volumes","cursor":"cursor"}`},
		{components.CreateV2QueryParamsQueryTemplateTransactionParams(components.QueryTemplateTransactionParams{Resource: components.V2QueryParamsResourceTransactions, PageSize: &size}), `{"resource":"transactions","pageSize":15}`},
		{components.CreateV2QueryParamsQueryTemplateLogParams(components.QueryTemplateLogParams{Resource: components.V2QueryParams3ResourceLogs, PageSize: &size}), `{"resource":"logs","pageSize":15}`},
		{components.CreateV2QueryParamsQueryTemplateVolumeParams(components.QueryTemplateVolumeParams{Resource: components.V2QueryParams4ResourceVolumes, GroupBy: &group, InsertionDate: &insertion}), `{"resource":"volumes","groupBy":2,"insertionDate":true}`},
	}
	for _, fixture := range fixtures {
		data, err := json.Marshal(fixture.value)
		require.NoError(t, err)
		require.JSONEq(t, fixture.expected, string(data))
		var decoded components.V2QueryParams
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, fixture.value.Type, decoded.Type)
	}
	// The parent field remains a pointer and the info response remains flat.
	resource := components.V2QueryResourceAccounts
	_ = components.V2QueryTemplate{Resource: &resource}
	_ = components.V2ConfigInfoResponse{Server: "ledger", Version: "v2.4.17"}
}

func TestQueryParamsRoundTripServerForms(t *testing.T) {
	for _, fixture := range []string{
		`{"pageSize":15,"sort":"address:asc"}`,
		`{"expand":["volumes"],"startTime":"2026-01-01T00:00:00Z","endTime":"2026-02-01T00:00:00Z"}`,
		`{"groupBy":2,"insertionDate":true}`,
	} {
		var params components.V2QueryParams
		require.NoError(t, json.Unmarshal([]byte(fixture), &params))
		require.Equal(t, components.V2QueryParamsTypeQueryTemplatePlainParams, params.Type)
		data, err := json.Marshal(params)
		require.NoError(t, err)
		require.JSONEq(t, fixture, string(data))
	}
}
