package transactions

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pterm/pterm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type transactionPagerClient struct {
	servicepb.BucketServiceClient

	calls int
}

func (c *transactionPagerClient) ListTransactions(context.Context, *servicepb.ListTransactionsRequest, ...grpc.CallOption) (servicepb.BucketService_ListTransactionsClient, error) {
	c.calls++

	return &transactionPagerStream{}, nil
}

type transactionPagerStream struct {
	grpc.ServerStreamingClient[commonpb.Transaction]

	read bool
}

func (s *transactionPagerStream) Recv() (*commonpb.Transaction, error) {
	if s.read {
		return nil, io.EOF
	}
	s.read = true

	return &commonpb.Transaction{Id: 42}, nil
}

func (*transactionPagerStream) Trailer() metadata.MD {
	return metadata.Pairs(cmdutil.NextCursorTrailerKey, "next-page")
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

	client := &transactionPagerClient{}
	cmd := NewListCommand()
	cmd.SetContext(context.Background())
	cmd.Flags().String("auth-token", "test", "")
	err = fetchTransactionsWithPager(cmd, client, "ledger", cmdutil.PaginationFlags{}, nil, cmdutil.ConsistencyFlags{}, false)
	if err != nil {
		t.Fatalf("pager should stop successfully: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("expected one page request, got %d", client.calls)
	}
	for _, want := range []string{"42", "next-page"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("missing %q in output: %q", want, output.String())
		}
	}
}
