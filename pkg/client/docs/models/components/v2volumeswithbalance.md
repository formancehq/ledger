# V2VolumesWithBalance


## Fields

| Field                                                   | Type                                                    | Required                                                | Description                                             |
| ------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------- |
| `Account`                                               | `string`                                                | :heavy_check_mark:                                      | The account address these volumes belong to             |
| `Asset`                                                 | `string`                                                | :heavy_check_mark:                                      | The asset these volumes are denominated in              |
| `Input`                                                 | [*big.Int](https://pkg.go.dev/math/big#Int)             | :heavy_check_mark:                                      | Total amount credited to the account for this asset     |
| `Output`                                                | [*big.Int](https://pkg.go.dev/math/big#Int)             | :heavy_check_mark:                                      | Total amount debited from the account for this asset    |
| `Balance`                                               | [*big.Int](https://pkg.go.dev/math/big#Int)             | :heavy_check_mark:                                      | Net balance for this asset, equal to input minus output |