# Formance.model.transaction.Transaction

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
**metadata** | [**LedgerMetadata**](LedgerMetadata.md) | [**LedgerMetadata**](LedgerMetadata.md) |  | [optional] 
**preCommitVolumes** | [**AggregatedVolumes**](AggregatedVolumes.md) | [**AggregatedVolumes**](AggregatedVolumes.md) |  | [optional] 
**postCommitVolumes** | [**AggregatedVolumes**](AggregatedVolumes.md) | [**AggregatedVolumes**](AggregatedVolumes.md) |  | [optional] 
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

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

