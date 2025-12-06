# Buckets
(*Buckets*)

## Overview

### Available Operations

* [ListBuckets](#listbuckets) - List all buckets
* [GetBucket](#getbucket) - Get a bucket
* [CreateBucket](#createbucket) - Create a new bucket
* [DeleteBucket](#deletebucket) - Delete a bucket

## ListBuckets

Returns a list of all buckets in the cluster

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

    res, err := s.Buckets.ListBuckets(ctx)
    if err != nil {
        log.Fatal(err)
    }
    if res.ListBucketsResponse != nil {
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

**[*operations.ListBucketsResponse](../../models/operations/listbucketsresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 500, 503                | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## GetBucket

Retrieves a bucket with its Raft cluster state

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Buckets.GetBucket(ctx, operations.GetBucketRequest{
        BucketName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.GetBucketResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                  | Type                                                                       | Required                                                                   | Description                                                                |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `ctx`                                                                      | [context.Context](https://pkg.go.dev/context#Context)                      | :heavy_check_mark:                                                         | The context to use for the request.                                        |
| `request`                                                                  | [operations.GetBucketRequest](../../models/operations/getbucketrequest.md) | :heavy_check_mark:                                                         | The request object to use for the request.                                 |
| `opts`                                                                     | [][operations.Option](../../models/operations/option.md)                   | :heavy_minus_sign:                                                         | The options for this request.                                              |

### Response

**[*operations.GetBucketResponse](../../models/operations/getbucketresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 404                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## CreateBucket

Creates a new bucket with the specified name, driver, and configuration

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Buckets.CreateBucket(ctx, operations.CreateBucketRequest{
        BucketName: "<value>",
        CreateBucketRequest: components.CreateBucketRequest{
            Driver: "<value>",
            Config: map[string]any{
                "key": "<value>",
                "key1": "<value>",
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.CreateBucketResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.CreateBucketRequest](../../models/operations/createbucketrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.CreateBucketResponse](../../models/operations/createbucketresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 400, 409                | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |

## DeleteBucket

Deletes a bucket with the specified name

### Example Usage

```go
package main

import(
	"context"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"log"
)

func main() {
    ctx := context.Background()

    s := client.New()

    res, err := s.Buckets.DeleteBucket(ctx, operations.DeleteBucketRequest{
        BucketName: "<value>",
    })
    if err != nil {
        log.Fatal(err)
    }
    if res.DeleteBucketResponse != nil {
        // handle response
    }
}
```

### Parameters

| Parameter                                                                        | Type                                                                             | Required                                                                         | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `ctx`                                                                            | [context.Context](https://pkg.go.dev/context#Context)                            | :heavy_check_mark:                                                               | The context to use for the request.                                              |
| `request`                                                                        | [operations.DeleteBucketRequest](../../models/operations/deletebucketrequest.md) | :heavy_check_mark:                                                               | The request object to use for the request.                                       |
| `opts`                                                                           | [][operations.Option](../../models/operations/option.md)                         | :heavy_minus_sign:                                                               | The options for this request.                                                    |

### Response

**[*operations.DeleteBucketResponse](../../models/operations/deletebucketresponse.md), error**

### Errors

| Error Type              | Status Code             | Content Type            |
| ----------------------- | ----------------------- | ----------------------- |
| sdkerrors.ErrorResponse | 404                     | application/json        |
| sdkerrors.ErrorResponse | 500                     | application/json        |
| sdkerrors.SDKError      | 4XX, 5XX                | \*/\*                   |