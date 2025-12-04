# PostingRequest


## Fields

| Field                                       | Type                                        | Required                                    | Description                                 |
| ------------------------------------------- | ------------------------------------------- | ------------------------------------------- | ------------------------------------------- |
| `Source`                                    | *string*                                    | :heavy_check_mark:                          | Source account address                      |
| `Destination`                               | *string*                                    | :heavy_check_mark:                          | Destination account address                 |
| `Amount`                                    | [*big.Int](https://pkg.go.dev/math/big#Int) | :heavy_check_mark:                          | Amount as a big integer                     |
| `Asset`                                     | *string*                                    | :heavy_check_mark:                          | Asset identifier                            |