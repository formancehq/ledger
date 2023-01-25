# Formance.model.dummy_pay_config.DummyPayConfig

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**directory** | str,  | str,  |  | 
**filePollingPeriod** | str,  | str,  | The frequency at which the connector will try to fetch new payment objects from the directory | [optional] if omitted the server will use the default value of "10s"
**fileGenerationPeriod** | str,  | str,  | The frequency at which the connector will create new payment objects in the directory | [optional] if omitted the server will use the default value of "10s"
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

