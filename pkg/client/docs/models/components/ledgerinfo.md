# LedgerInfo

Runtime information about a ledger, including the state of its storage migrations


## Fields

| Field                                                         | Type                                                          | Required                                                      | Description                                                   | Example                                                       |
| ------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------- |
| `Name`                                                        | `*string`                                                     | :heavy_minus_sign:                                            | Name of the ledger                                            | ledger001                                                     |
| `Storage`                                                     | [*components.Storage](../../models/components/storage.md)     | :heavy_minus_sign:                                            | Storage backend information, including the applied migrations |                                                               |