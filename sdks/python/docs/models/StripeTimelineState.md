# Formance.model.stripe_timeline_state.StripeTimelineState

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**oldestId** | str,  | str,  | The id of the oldest BalanceTransaction fetched from stripe for this account | [optional] 
**oldestDate** | str, datetime,  | str,  | The creation date of the oldest BalanceTransaction fetched from stripe for this account | [optional] value must conform to RFC-3339 date-time
**moreRecentId** | str,  | str,  | The id of the more recent BalanceTransaction fetched from stripe for this account | [optional] 
**moreRecentDate** | str, datetime,  | str,  | The creation date of the more recent BalanceTransaction fetched from stripe for this account | [optional] value must conform to RFC-3339 date-time
**noMoreHistory** | bool,  | BoolClass,  |  | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

