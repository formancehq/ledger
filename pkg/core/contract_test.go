package core

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContract_UnmarshalJSON(t *testing.T) {
	contract := &Contract{}
	data := `{"id": "foo", "account": "order:*", "expr": { "$gte": ["$balance", 0] }}`
	err := json.Unmarshal([]byte(data), contract)
	assert.NoError(t, err)
}
