package queries

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/pterm/pterm"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type queryPagerClient struct {
	servicepb.BucketServiceClient

	calls int
}

func (c *queryPagerClient) ExecutePreparedQuery(context.Context, *servicepb.ExecutePreparedQueryRequest, ...grpc.CallOption) (*servicepb.ExecutePreparedQueryResponse, error) {
	c.calls++

	return &servicepb.ExecutePreparedQueryResponse{
		Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: &commonpb.PreparedQueryCursor{
			HasMore:     true,
			Next:        "next-page",
			AccountData: []*commonpb.Account{{Address: "users:alice"}},
		}},
	}, nil
}

func TestPagerStopsAfterFirstPageAtEOF(t *testing.T) {
	stdin, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	t.Cleanup(func() { _ = stdin.Close() })
	oldStdin := os.Stdin
	os.Stdin = stdin
	t.Cleanup(func() { os.Stdin = oldStdin })

	var output bytes.Buffer
	pterm.SetDefaultOutput(&output)
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout) })
	infoWriter := pterm.Info.Writer
	pterm.Info.Writer = &output
	t.Cleanup(func() { pterm.Info.Writer = infoWriter })

	client := &queryPagerClient{}
	cmd := NewExecuteCommand()
	cmd.SetContext(context.Background())
	cmd.Flags().String("auth-token", "test", "")
	err = executeWithPager(cmd, client, &servicepb.ExecutePreparedQueryRequest{Ledger: "ledger", QueryName: "test"}, false)
	if err != nil {
		t.Fatalf("pager should stop successfully: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("expected one page request, got %d", client.calls)
	}
	for _, want := range []string{"users:alice", "rerun with --all"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("missing %q in output: %q", want, output.String())
		}
	}
}
