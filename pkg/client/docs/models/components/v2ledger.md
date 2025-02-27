# V2Ledger


## Fields

| Field                                     | Type                                      | Required                                  | Description                               | Example                                   |
| ----------------------------------------- | ----------------------------------------- | ----------------------------------------- | ----------------------------------------- | ----------------------------------------- |
| `Name`                                    | *string*                                  | :heavy_check_mark:                        | N/A                                       |                                           |
| `AddedAt`                                 | [time.Time](https://pkg.go.dev/time#Time) | :heavy_check_mark:                        | N/A                                       |                                           |
| `Bucket`                                  | *string*                                  | :heavy_check_mark:                        | N/A                                       |                                           |
| `Metadata`                                | map[string]*string*                       | :heavy_minus_sign:                        | N/A                                       | {<br/>"admin": "true"<br/>}               |
| `Features`                                | map[string]*string*                       | :heavy_minus_sign:                        | N/A                                       |                                           |
| `ID`                                      | *int64*                                   | :heavy_check_mark:                        | N/A                                       |                                           |