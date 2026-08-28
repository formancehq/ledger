# Volume


## Fields

| Field                                       | Type                                        | Required                                    | Description                                 |
| ------------------------------------------- | ------------------------------------------- | ------------------------------------------- | ------------------------------------------- |
| `Input`                                     | [*big.Int](https://pkg.go.dev/math/big#Int) | :heavy_check_mark:                          | Total amount credited for this asset        |
| `Output`                                    | [*big.Int](https://pkg.go.dev/math/big#Int) | :heavy_check_mark:                          | Total amount debited for this asset         |
| `Balance`                                   | [*big.Int](https://pkg.go.dev/math/big#Int) | :heavy_minus_sign:                          | Net balance, equal to input minus output    |