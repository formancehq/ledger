package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTxsToScriptsData(t *testing.T) {
	ts := time.Now()
	tests := []struct {
		name   string
		input  []TransactionData
		output []ScriptData
	}{
		{
			name:   "empty",
			input:  []TransactionData{},
			output: []ScriptData{},
		},
		{
			name: "nominal",
			input: []TransactionData{
				{
					Postings: Postings{
						{
							Source:      "world",
							Destination: "alice",
							Asset:       "EUR/2",
							Amount:      NewMonetaryInt(100),
						},
					},
					Reference: "ref",
					Timestamp: ts,
					Metadata:  Metadata{"key": "val"},
				},
			},
			output: []ScriptData{
				{
					Script: Script{
						Plain: "vars {\n\taccount $va0\n\tmonetary $vm0\n}\nsend $vm0 (\n\tsource = @world\n\tdestination = $va0\n)\n",
						Vars: map[string]json.RawMessage{
							"va0": json.RawMessage(`"alice"`),
							"vm0": json.RawMessage(`{"asset":"EUR/2","amount":100}`),
						},
					},
					Reference: "ref",
					Timestamp: ts,
					Metadata:  Metadata{"key": "val"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.output,
				TxsToScriptsData(tt.input...))
		})
	}
}
