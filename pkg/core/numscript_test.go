package core

import (
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
						Plain: "send [EUR/2 100] (\n\tsource = @world\n\tdestination = @alice\n)\n",
					},
					Reference: "ref",
					Timestamp: ts,
					Metadata:  Metadata{"key": "val"},
				},
			},
		},
		{
			name: "multiple postings",
			input: []TransactionData{
				{
					Postings: Postings{
						{
							Source:      "world",
							Destination: "alice",
							Asset:       "EUR/2",
							Amount:      NewMonetaryInt(100),
						},
						{
							Source:      "world",
							Destination: "bob",
							Asset:       "USD/2",
							Amount:      NewMonetaryInt(1000),
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
						Plain: "send [EUR/2 100] (\n\tsource = @world\n\tdestination = @alice\n)\n" +
							"send [USD/2 1000] (\n\tsource = @world\n\tdestination = @bob\n)\n",
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
