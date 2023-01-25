# Formance.model.payment_adjustment.PaymentAdjustment

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**date** | str, datetime,  | str,  |  | value must conform to RFC-3339 date-time
**amount** | decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer
**absolute** | bool,  | BoolClass,  |  | 
**[raw](#raw)** | dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 
**status** | [**PaymentStatus**](PaymentStatus.md) | [**PaymentStatus**](PaymentStatus.md) |  | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# raw

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

