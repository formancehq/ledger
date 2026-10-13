# V2SchemasCursor

Paginated cursor over the ledger's schemas


## Fields

| Field                                                        | Type                                                         | Required                                                     | Description                                                  | Example                                                      |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `Data`                                                       | [][components.V2Schema](../../models/components/v2schema.md) | :heavy_check_mark:                                           | The schemas on this page                                     |                                                              |
| `HasMore`                                                    | `bool`                                                       | :heavy_check_mark:                                           | Whether further pages are available                          |                                                              |
| `Previous`                                                   | `*string`                                                    | :heavy_minus_sign:                                           | Cursor for the previous page, absent on the first page       | YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=                 |
| `Next`                                                       | `*string`                                                    | :heavy_minus_sign:                                           | Cursor for the next page, absent on the last page            | aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=                             |
| `PageSize`                                                   | `int64`                                                      | :heavy_check_mark:                                           | Number of items requested per page                           |                                                              |