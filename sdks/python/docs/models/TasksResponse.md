# Formance.model.tasks_response.TasksResponse

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**[cursor](#cursor)** | dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# cursor

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**[data](#data)** | list, tuple,  | tuple,  |  | 
**hasMore** | bool,  | BoolClass,  |  | 
**pageSize** | decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer
**previous** | str,  | str,  |  | [optional] 
**next** | str,  | str,  |  | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# data

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[items](#items) | dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

# items

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

