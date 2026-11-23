package queries

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type logQueryClient struct {
	servicepb.BucketServiceClient

	cursors []string
}

func (c *logQueryClient) ExecutePreparedQuery(_ context.Context, req *servicepb.ExecutePreparedQueryRequest, _ ...grpc.CallOption) (*servicepb.ExecutePreparedQueryResponse, error) {
	c.cursors = append(c.cursors, req.GetCursor())
	sequence := uint64(len(c.cursors))

	return &servicepb.ExecutePreparedQueryResponse{Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: &commonpb.PreparedQueryCursor{
		LogData: []*commonpb.Log{{Sequence: sequence, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "ledger", Log: &commonpb.LedgerLog{Id: sequence + 10, Date: &commonpb.Timestamp{Data: 1_700_000_000_000_000}, Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: sequence}}}}}}}}}},
		HasMore: sequence == 1,
		Next:    "next-page",
	}}}, nil
}

func captureLogQueryOutput(t *testing.T, run func() error) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = writer
	var human bytes.Buffer
	pterm.SetDefaultOutput(&human)
	oldInfo := pterm.Info.Writer
	pterm.Info.Writer = &human
	t.Cleanup(func() {
		os.Stdout = oldStdout
		pterm.SetDefaultOutput(oldStdout)
		pterm.Info.Writer = oldInfo
	})
	require.NoError(t, run())
	require.NoError(t, writer.Close())
	structured, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	return human.String() + string(structured)
}

func TestExecuteLogResults(t *testing.T) {
	for _, format := range []string{"human", "json", "yaml"} {
		for _, all := range []bool{false, true} {
			t.Run(format+"/all="+strings.ToUpper(strconv.FormatBool(all)), func(t *testing.T) {
				client := &logQueryClient{}
				cmd := NewExecuteCommand()
				cmd.SetContext(context.Background())
				cmd.Flags().String("auth-token", "test", "")
				if format != "human" {
					require.NoError(t, cmd.Flags().Set(format, "true"))
				}
				output := captureLogQueryOutput(t, func() error {
					req := &servicepb.ExecutePreparedQueryRequest{Ledger: "ledger", QueryName: "logs"}
					if all {
						return executeAll(cmd, client, req, false)
					}

					return executeWithPager(cmd, client, req, false)
				})
				first, second := "11", "12"
				switch format {
				case "json":
					first, second = `"sequence": 1`, `"sequence": 2`
				case "yaml":
					first, second = "sequence: 1", "sequence: 2"
				}
				require.Contains(t, output, first)
				require.NotContains(t, output, "No results")
				if all {
					require.Equal(t, []string{"", "next-page"}, client.cursors)
					require.Contains(t, output, second)
				} else {
					require.Equal(t, []string{""}, client.cursors)
				}
			})
		}
	}
}
