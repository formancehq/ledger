# LogsCursorResponseCursor


## Fields

| Field                                              | Type                                               | Required                                           | Description                                        | Example                                            |
| -------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- |
| `PageSize`                                         | *int64*                                            | :heavy_check_mark:                                 | N/A                                                | 15                                                 |
| `HasMore`                                          | *bool*                                             | :heavy_check_mark:                                 | N/A                                                | false                                              |
| `Previous`                                         | **string*                                          | :heavy_minus_sign:                                 | N/A                                                | YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=       |
| `Next`                                             | **string*                                          | :heavy_minus_sign:                                 | N/A                                                |                                                    |
| `Data`                                             | [][components.Log](../../models/components/log.md) | :heavy_check_mark:                                 | N/A                                                |                                                    |