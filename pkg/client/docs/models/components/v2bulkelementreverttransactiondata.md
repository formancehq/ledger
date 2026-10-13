# V2BulkElementRevertTransactionData


## Fields

| Field                                              | Type                                               | Required                                           | Description                                        | Example                                            |
| -------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- |
| `ID`                                               | [*big.Int](https://pkg.go.dev/math/big#Int)        | :heavy_check_mark:                                 | N/A                                                |                                                    |
| `Force`                                            | `*bool`                                            | :heavy_minus_sign:                                 | N/A                                                |                                                    |
| `AtEffectiveDate`                                  | `*bool`                                            | :heavy_minus_sign:                                 | N/A                                                |                                                    |
| `Metadata`                                         | map[string]`string`                                | :heavy_minus_sign:                                 | Arbitrary key/value pairs attached to the resource | {<br/>"admin": "true"<br/>}                        |