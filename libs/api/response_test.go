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

	c = Cursor[int64]{
		Data:    []int64{1, 2, 3},
		HasMore: true,
	}
	by, err = json.Marshal(c)
	require.NoError(t, err)
	assert.Equal(t,
		`{"hasMore":true,"data":[1,2,3]}`,
		string(by))
}
