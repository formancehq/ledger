# V2LogDataRevertedTransaction

Payload for REVERTED_TRANSACTION log entries. Contains both the original reverted transaction and the new reverting transaction.


## Fields

| Field                                                                      | Type                                                                       | Required                                                                   | Description                                                                |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `RevertedTransaction`                                                      | [components.V2LogTransaction](../../models/components/v2logtransaction.md) | :heavy_check_mark:                                                         | The original transaction that was reverted                                 |
| `Transaction`                                                              | [components.V2LogTransaction](../../models/components/v2logtransaction.md) | :heavy_check_mark:                                                         | The new reverting transaction created to cancel the original               |