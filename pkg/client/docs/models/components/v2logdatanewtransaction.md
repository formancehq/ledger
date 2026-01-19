# V2LogDataNewTransaction

Payload for NEW_TRANSACTION log entries. Contains the created transaction and any account metadata set during creation.


## Fields

| Field                                                                      | Type                                                                       | Required                                                                   | Description                                                                |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `Transaction`                                                              | [components.V2LogTransaction](../../models/components/v2logtransaction.md) | :heavy_check_mark:                                                         | Transaction structure as it appears in log payloads                        |
| `AccountMetadata`                                                          | map[string]map[string]*string*                                             | :heavy_check_mark:                                                         | Metadata applied to accounts involved in the transaction                   |