package bulking

import (
	"bufio"
	"bytes"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseStream(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name             string
		stream           string
		expectedError    bool
		expectedElements []BulkElement
	}

	for _, testCase := range []testCase{
		{
			name: "nominal",
			expectedElements: []BulkElement{
				{
					Action: ActionCreateTransaction,
					Data: TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: ledgercontroller.Script{
								Plain: `send [USD 100] (
	source = @world
	destination = @alice
)`,
							},
						},
					},
				},
			},
			stream: `
//script
send [USD 100] (
	source = @world
	destination = @alice
)
//end`,
		},
		{
			name: "multiple scripts",
			expectedElements: []BulkElement{
				{
					Action: ActionCreateTransaction,
					Data: TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: ledgercontroller.Script{
								Plain: `send [USD 100] (
	source = @world
	destination = @alice
)`,
							},
						},
					},
				},
				{
					Action: ActionCreateTransaction,
					Data: TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: ledgercontroller.Script{
								Plain: `send [USD 100] (
	source = @world
	destination = @bob
)`,
							},
						},
					},
				},
			},
			stream: `
//script
send [USD 100] (
	source = @world
	destination = @alice
)
//end
//script
send [USD 100] (
	source = @world
	destination = @bob
)
//end`,
		},
		{
			name: "no tags",
			stream: `
send [USD 100] (
	source = @world
	destination = @alice
)`,
			expectedError: true,
		},
		{
			name: "no ending tag",
			expectedElements: []BulkElement{
				{
					Action: ActionCreateTransaction,
					Data: TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: ledgercontroller.Script{
								Plain: `send [USD 100] (
	source = @world
	destination = @alice
)`,
							},
						},
					},
				},
			},
			stream: `
//script
send [USD 100] (
	source = @world
	destination = @alice
)`,
		},
		{
			name: "script with ik",
			expectedElements: []BulkElement{
				{
					Action:         ActionCreateTransaction,
					IdempotencyKey: "foo",
					Data: TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: ledgercontroller.Script{
								Plain: `send [USD 100] (
	source = @world
	destination = @alice
)`,
							},
						},
					},
				},
			},
			stream: `
//script ik=foo
send [USD 100] (
	source = @world
	destination = @alice
)
//end`,
		},
		{
			name:          "script with ik specified twice",
			expectedError: true,
			stream: `
//script ik=foo,ik=bar
send [USD 100] (
	source = @world
	destination = @alice
)
//end`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if testCase.expectedError {
				_, err := ParseTextStream(bufio.NewScanner(bytes.NewBufferString(testCase.stream)))
				require.Error(t, err)
				return
			} else {
				scanner := bufio.NewScanner(bytes.NewBufferString(testCase.stream))
				for _, element := range testCase.expectedElements {
					ret, err := ParseTextStream(scanner)
					require.NoError(t, err)
					require.NotNil(t, ret)
					require.Equal(t, element, *ret)
				}
			}
		})
	}
}
