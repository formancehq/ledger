# V2BulkElementResultError


## Fields

| Field                                                | Type                                                 | Required                                             | Description                                          |
| ---------------------------------------------------- | ---------------------------------------------------- | ---------------------------------------------------- | ---------------------------------------------------- |
| `ResponseType`                                       | `string`                                             | :heavy_check_mark:                                   | The action this result corresponds to                |
| `LogID`                                              | `int64`                                              | :heavy_check_mark:                                   | Identifier of the log entry produced by this element |
| `ErrorCode`                                          | `string`                                             | :heavy_check_mark:                                   | N/A                                                  |
| `ErrorDescription`                                   | `string`                                             | :heavy_check_mark:                                   | N/A                                                  |
| `ErrorDetails`                                       | `*string`                                            | :heavy_minus_sign:                                   | N/A                                                  |