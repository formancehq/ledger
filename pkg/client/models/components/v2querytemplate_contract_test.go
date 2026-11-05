package components_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/client/models/components"
)

func TestV2QueryTemplateRoundTripsServerForms(t *testing.T) {
	t.Parallel()

	for _, fixture := range []struct {
		source string
		typ    components.V2QueryTemplateType
	}{
		{`{"resource":"accounts","params":{"pageSize":15,"sort":"address:asc","expand":["volumes"],"startTime":"2026-01-01T00:00:00Z","endTime":"2026-02-01T00:00:00Z"}}`, components.V2QueryTemplateTypeAccounts},
		{`{"resource":"transactions","params":{"pageSize":15}}`, components.V2QueryTemplateTypeTransactions},
		{`{"resource":"logs","body":{"$match":{"type":"NEW_TRANSACTION"}},"vars":{"x":{"type":"string"}}}`, components.V2QueryTemplateTypeLogs},
		{`{"resource":"volumes","params":{"groupBy":2,"insertionDate":true}}`, components.V2QueryTemplateTypeVolumes},
	} {
		var template components.V2QueryTemplate
		require.NoError(t, json.Unmarshal([]byte(fixture.source), &template), fixture.source)
		require.Equal(t, fixture.typ, template.Type, fixture.source)

		data, err := json.Marshal(template)
		require.NoError(t, err)
		require.JSONEq(t, fixture.source, string(data))
	}

	var template components.V2QueryTemplate
	require.Error(t, json.Unmarshal([]byte(`{"params":{"pageSize":1}}`), &template))
}

func TestV2QueryTemplateConstructorsSetResource(t *testing.T) {
	t.Parallel()

	groupBy := int64(2)
	data, err := json.Marshal(components.CreateV2QueryTemplateVolumes(components.V2VolumeQueryTemplate{
		Params: &components.V2VolumeQueryParams{GroupBy: &groupBy},
	}))
	require.NoError(t, err)
	require.JSONEq(t, `{"resource":"volumes","params":{"groupBy":2}}`, string(data))
}
