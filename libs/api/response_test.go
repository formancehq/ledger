package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursor(t *testing.T) {
	c := Cursor[int64]{
		Data: []int64{1, 2, 3},
	}
	by, err := json.Marshal(c)
	require.NoError(t, err)
	assert.Equal(t, `{"hasMore":false,"data":[1,2,3]}`, string(by))

	hasMore := true
	c = Cursor[int64]{
		Data: []int64{1, 2, 3},
		Total: &Total{
			Value: 10,
			Rel:   "eq",
		},
		HasMoreDeprecated: &hasMore,
	}
	by, err = json.Marshal(c)
	require.NoError(t, err)
	assert.Equal(t,
		`{"total":{"value":10,"relation":"eq"},"hasMore":false,"data":[1,2,3],"has_more":true}`,
		string(by))
}
