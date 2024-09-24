# TransactionData


## Fields

| Field                                                      | Type                                                       | Required                                                   | Description                                                | Example                                                    |
| ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- |
| `Postings`                                                 | [][components.Posting](../../models/components/posting.md) | :heavy_check_mark:                                         | N/A                                                        |                                                            |
| `Reference`                                                | **string*                                                  | :heavy_minus_sign:                                         | N/A                                                        | ref:001                                                    |
| `Metadata`                                                 | map[string]*any*                                           | :heavy_minus_sign:                                         | N/A                                                        |                                                            |
| `Timestamp`                                                | [*time.Time](https://pkg.go.dev/time#Time)                 | :heavy_minus_sign:                                         | N/A                                                        |                                                            |