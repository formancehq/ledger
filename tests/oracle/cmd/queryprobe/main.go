// Command queryprobe runs one filtered ListTransactions against a server and
// prints the returned ids — a minimal reproduction driver for index findings.
//
// Usage: queryprobe <server> <ledger> <account> <role:any|src|dst>
package main

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func main() {
	server, ledger, account, roleArg := os.Args[1], os.Args[2], os.Args[3], os.Args[4]

	role := commonpb.AddressRole_ADDRESS_ROLE_ANY
	switch roleArg {
	case "src":
		role = commonpb.AddressRole_ADDRESS_ROLE_SOURCE
	case "dst":
		role = commonpb.AddressRole_ADDRESS_ROLE_DESTINATION
	}

	conn, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := servicepb.NewBucketServiceClient(conn)

	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: account},
		Role:  role,
	}}}

	stream, err := client.ListTransactions(context.Background(), &servicepb.ListTransactionsRequest{
		Ledger:  ledger,
		Options: &commonpb.ListOptions{PageSize: 100, Filter: filter},
	})
	if err != nil {
		fmt.Println("ERR:", err)

		return
	}

	var ids []uint64
	for {
		tx, err := stream.Recv()
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("stream end:", err)
			}

			break
		}
		ids = append(ids, tx.GetId())
	}
	fmt.Println("ids:", ids)
}
