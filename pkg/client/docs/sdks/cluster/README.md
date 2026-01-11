# Cluster
(*Cluster*)

## Overview

### Available Operations

* [GetClusterState](#getclusterstate) - Get cluster state

## GetClusterState

Returns the current state of the Raft cluster, including the list of nodes and the current leader

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Cluster.GetClusterState(ctx)
    if err != nil {
        log.Fatal(err)
    }
    if res.ClusterStateResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                | Type                                                     | Required                                                 | Description                                              |
| -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `ctx`                                                    | [context.Context](https://pkg.go.dev/context#Context)    | :heavy_check_mark:                                       | The context to use for the request.                      |
| `opts`                                                   | [][operations.Option](../../models/operations/option.md) | :heavy_minus_sign:                                       | The options for this request.                            |

### Response

**[*operations.GetClusterStateResponse](../../models/operations/getclusterstateresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 405                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.ErrorResponse | 503                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |