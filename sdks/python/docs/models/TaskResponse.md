# Formance.model.task_response.TaskResponse

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**[data](#data)** | dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# data

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

### Composed Schemas (allOf/anyOf/oneOf/not)
#### oneOf
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[TaskStripe](TaskStripe.md) | [**TaskStripe**](TaskStripe.md) | [**TaskStripe**](TaskStripe.md) |  | 
[TaskWise](TaskWise.md) | [**TaskWise**](TaskWise.md) | [**TaskWise**](TaskWise.md) |  | 
[TaskCurrencyCloud](TaskCurrencyCloud.md) | [**TaskCurrencyCloud**](TaskCurrencyCloud.md) | [**TaskCurrencyCloud**](TaskCurrencyCloud.md) |  | 
[TaskDummyPay](TaskDummyPay.md) | [**TaskDummyPay**](TaskDummyPay.md) | [**TaskDummyPay**](TaskDummyPay.md) |  | 
[TaskModulr](TaskModulr.md) | [**TaskModulr**](TaskModulr.md) | [**TaskModulr**](TaskModulr.md) |  | 
[TaskBankingCircle](TaskBankingCircle.md) | [**TaskBankingCircle**](TaskBankingCircle.md) | [**TaskBankingCircle**](TaskBankingCircle.md) |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

