# Formance.model.wallets_transaction.WalletsTransaction

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**[postings](#postings)** | list, tuple,  | tuple,  |  | 
**txid** | decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer
**timestamp** | str, datetime,  | str,  |  | value must conform to RFC-3339 date-time
**reference** | str,  | str,  |  | [optional] 
**[metadata](#metadata)** | dict, frozendict.frozendict,  | frozendict.frozendict,  | Metadata associated with the wallet. | [optional] 
**preCommitVolumes** | [**WalletsAggregatedVolumes**](WalletsAggregatedVolumes.md) | [**WalletsAggregatedVolumes**](WalletsAggregatedVolumes.md) |  | [optional] 
**postCommitVolumes** | [**WalletsAggregatedVolumes**](WalletsAggregatedVolumes.md) | [**WalletsAggregatedVolumes**](WalletsAggregatedVolumes.md) |  | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# postings

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**Posting**](Posting.md) | [**Posting**](Posting.md) | [**Posting**](Posting.md) |  | 

# metadata

Metadata associated with the wallet.

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | Metadata associated with the wallet. | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

