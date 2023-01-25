# Formance.model.payment.Payment

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**[metadata](#metadata)** | list, tuple,  | tuple,  |  | 
**[adjustments](#adjustments)** | list, tuple,  | tuple,  |  | 
**scheme** | str,  | str,  |  | must be one of ["visa", "mastercard", "amex", "diners", "discover", "jcb", "unionpay", "sepa debit", "sepa credit", "sepa", "apple pay", "google pay", "a2a", "ach debit", "ach", "rtp", "unknown", "other", ] 
**[raw](#raw)** | dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 
**type** | str,  | str,  |  | must be one of ["PAY-IN", "PAYOUT", "TRANSFER", "OTHER", ] 
**reference** | str,  | str,  |  | 
**accountID** | str,  | str,  |  | 
**createdAt** | str, datetime,  | str,  |  | value must conform to RFC-3339 date-time
**provider** | [**Connector**](Connector.md) | [**Connector**](Connector.md) |  | 
**initialAmount** | decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer
**id** | str,  | str,  |  | 
**asset** | str,  | str,  |  | 
**status** | [**PaymentStatus**](PaymentStatus.md) | [**PaymentStatus**](PaymentStatus.md) |  | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# raw

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

# adjustments

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**PaymentAdjustment**](PaymentAdjustment.md) | [**PaymentAdjustment**](PaymentAdjustment.md) | [**PaymentAdjustment**](PaymentAdjustment.md) |  | 

# metadata

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**PaymentMetadata**](PaymentMetadata.md) | [**PaymentMetadata**](PaymentMetadata.md) | [**PaymentMetadata**](PaymentMetadata.md) |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

