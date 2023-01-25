# Formance.model.stripe_config.StripeConfig

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**apiKey** | str,  | str,  |  | 
**pollingPeriod** | str,  | str,  | The frequency at which the connector will try to fetch new BalanceTransaction objects from Stripe API.  | [optional] if omitted the server will use the default value of "120s"
**pageSize** | decimal.Decimal, int,  | decimal.Decimal,  | Number of BalanceTransaction to fetch at each polling interval.  | [optional] if omitted the server will use the default value of 10value must be a 64 bit integer
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

