// Command logprobe streams a ledger's logs over gRPC and prints the raw
// LedgerLog volume annotation lists (purged/ephemeral/new-kept) for the given
// global sequences — the exclusion-projection inputs the index builder
// consumes, which the JSON adapter does not render.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func main() {
	server, ledger := os.Args[1], os.Args[2]
	want := map[uint64]bool{}
	for _, a := range os.Args[3:] {
		s, _ := strconv.ParseUint(a, 10, 64)
		want[s] = true
	}

	conn, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := servicepb.NewBucketServiceClient(conn)

	stream, err := client.ListLogs(context.Background(), &servicepb.ListLogsRequest{
		Ledger:  ledger,
		Options: &commonpb.ListOptions{PageSize: 2000},
	})
	if err != nil {
		panic(err)
	}

	for {
		log, err := stream.Recv()
		if err != nil {
			break
		}
		if !want[log.GetSequence()] {
			continue
		}

		ll := log.GetPayload().GetApply().GetLog()
		fmt.Printf("seq=%d id=%d purged=%v ephemeral=%v newKept=%v\n",
			log.GetSequence(), ll.GetId(), ll.GetPurgedVolumes(), ll.GetEphemeralVolumes(), ll.GetNewKeptVolumes())
	}
}
