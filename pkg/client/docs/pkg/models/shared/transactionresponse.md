# TransactionResponse


## Fields

| Field                                                                     | Type                                                                      | Required                                                                  | Description                                                               |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `ID`                                                                      | **int64*                                                                  | :heavy_minus_sign:                                                        | Transaction ID (assigned by the system)                                   |
| `Metadata`                                                                | map[string]*any*                                                          | :heavy_minus_sign:                                                        | N/A                                                                       |
| `Postings`                                                                | [][shared.PostingResponse](../../../pkg/models/shared/postingresponse.md) | :heavy_minus_sign:                                                        | N/A                                                                       |
| `Reference`                                                               | **string*                                                                 | :heavy_minus_sign:                                                        | N/A                                                                       |
| `Timestamp`                                                               | [*time.Time](https://pkg.go.dev/time#Time)                                | :heavy_minus_sign:                                                        | Transaction timestamp (ISO 8601 format)                                   |