# V2GetTransactionRequest


## Fields

| Field                                       | Type                                        | Required                                    | Description                                 | Example                                     |
| ------------------------------------------- | ------------------------------------------- | ------------------------------------------- | ------------------------------------------- | ------------------------------------------- |
| `Ledger`                                    | *string*                                    | :heavy_check_mark:                          | Name of the ledger.                         | ledger001                                   |
| `ID`                                        | [*big.Int](https://pkg.go.dev/math/big#Int) | :heavy_check_mark:                          | Transaction ID.                             | 1234                                        |
| `Expand`                                    | **string*                                   | :heavy_minus_sign:                          | N/A                                         |                                             |
| `Pit`                                       | [*time.Time](https://pkg.go.dev/time#Time)  | :heavy_minus_sign:                          | N/A                                         |                                             |