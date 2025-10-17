# V2ListSchemasRequest


## Fields

| Field                                                 | Type                                                  | Required                                              | Description                                           | Example                                               |
| ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- |
| `Ledger`                                              | *string*                                              | :heavy_check_mark:                                    | Name of the ledger.                                   | ledger001                                             |
| `Cursor`                                              | **string*                                             | :heavy_minus_sign:                                    | The pagination cursor value                           |                                                       |
| `PageSize`                                            | **int64*                                              | :heavy_minus_sign:                                    | The maximum number of results to return per page      |                                                       |
| `Sort`                                                | [*operations.Sort](../../models/operations/sort.md)   | :heavy_minus_sign:                                    | The field to sort by                                  |                                                       |
| `Order`                                               | [*operations.Order](../../models/operations/order.md) | :heavy_minus_sign:                                    | The sort order                                        |                                                       |