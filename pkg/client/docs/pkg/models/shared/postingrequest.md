# PostingRequest


## Fields

| Field                                       | Type                                        | Required                                    | Description                                 |
| ------------------------------------------- | ------------------------------------------- | ------------------------------------------- | ------------------------------------------- |
| `Amount`                                    | [*big.Int](https://pkg.go.dev/math/big#Int) | :heavy_check_mark:                          | Amount as a big integer                     |
| `Asset`                                     | *string*                                    | :heavy_check_mark:                          | Asset identifier                            |
| `Destination`                               | *string*                                    | :heavy_check_mark:                          | Destination account address                 |
| `Source`                                    | *string*                                    | :heavy_check_mark:                          | Source account address                      |