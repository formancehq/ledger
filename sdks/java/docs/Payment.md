

# Payment


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**id** | **String** |  |  |
|**reference** | **String** |  |  |
|**accountID** | **String** |  |  |
|**type** | [**TypeEnum**](#TypeEnum) |  |  |
|**provider** | **Connector** |  |  |
|**status** | **PaymentStatus** |  |  |
|**initialAmount** | **Long** |  |  |
|**scheme** | [**SchemeEnum**](#SchemeEnum) |  |  |
|**asset** | **String** |  |  |
|**createdAt** | **OffsetDateTime** |  |  |
|**raw** | **Object** |  |  |
|**adjustments** | [**List&lt;PaymentAdjustment&gt;**](PaymentAdjustment.md) |  |  |
|**metadata** | [**List&lt;PaymentMetadata&gt;**](PaymentMetadata.md) |  |  |



## Enum: TypeEnum

| Name | Value |
|---- | -----|
| PAY_IN | &quot;PAY-IN&quot; |
| PAYOUT | &quot;PAYOUT&quot; |
| TRANSFER | &quot;TRANSFER&quot; |
| OTHER | &quot;OTHER&quot; |



## Enum: SchemeEnum

| Name | Value |
|---- | -----|
| VISA | &quot;visa&quot; |
| MASTERCARD | &quot;mastercard&quot; |
| AMEX | &quot;amex&quot; |
| DINERS | &quot;diners&quot; |
| DISCOVER | &quot;discover&quot; |
| JCB | &quot;jcb&quot; |
| UNIONPAY | &quot;unionpay&quot; |
| SEPA_DEBIT | &quot;sepa debit&quot; |
| SEPA_CREDIT | &quot;sepa credit&quot; |
| SEPA | &quot;sepa&quot; |
| APPLE_PAY | &quot;apple pay&quot; |
| GOOGLE_PAY | &quot;google pay&quot; |
| A2A | &quot;a2a&quot; |
| ACH_DEBIT | &quot;ach debit&quot; |
| ACH | &quot;ach&quot; |
| RTP | &quot;rtp&quot; |
| UNKNOWN | &quot;unknown&quot; |
| OTHER | &quot;other&quot; |



