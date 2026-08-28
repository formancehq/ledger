# V2Stats

Aggregate counts for a ledger


## Fields

| Field                                       | Type                                        | Required                                    | Description                                 |
| ------------------------------------------- | ------------------------------------------- | ------------------------------------------- | ------------------------------------------- |
| `Accounts`                                  | `int64`                                     | :heavy_check_mark:                          | Total number of accounts in the ledger      |
| `Transactions`                              | [*big.Int](https://pkg.go.dev/math/big#Int) | :heavy_check_mark:                          | Total number of transactions in the ledger  |