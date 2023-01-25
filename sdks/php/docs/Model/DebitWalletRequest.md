# # DebitWalletRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**amount** | [**\Formance\Model\Monetary**](Monetary.md) |  |
**pending** | **bool** | Set to true to create a pending hold. If false, the wallet will be debited immediately. | [optional]
**metadata** | **array<string,mixed>** | Metadata associated with the wallet. | [optional]
**description** | **string** |  | [optional]
**destination** | [**\Formance\Model\Subject**](Subject.md) |  | [optional]
**balances** | **string[]** |  | [optional]

[[Back to Model list]](../../README.md#models) [[Back to API list]](../../README.md#endpoints) [[Back to README]](../../README.md)
