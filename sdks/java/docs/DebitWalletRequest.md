

# DebitWalletRequest


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**amount** | [**Monetary**](Monetary.md) |  |  |
|**pending** | **Boolean** | Set to true to create a pending hold. If false, the wallet will be debited immediately. |  [optional] |
|**metadata** | **Map&lt;String, Object&gt;** | Metadata associated with the wallet. |  [optional] |
|**description** | **String** |  |  [optional] |
|**destination** | [**Subject**](Subject.md) |  |  [optional] |
|**balances** | **List&lt;String&gt;** |  |  [optional] |



