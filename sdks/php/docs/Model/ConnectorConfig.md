# # ConnectorConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**polling_period** | **string** | The frequency at which the connector will fetch transactions | [optional]
**api_key** | **string** |  |
**page_size** | **int** | Number of BalanceTransaction to fetch at each polling interval. | [optional] [default to 10]
**file_polling_period** | **string** | The frequency at which the connector will try to fetch new payment objects from the directory | [optional] [default to '10s']
**file_generation_period** | **string** | The frequency at which the connector will create new payment objects in the directory | [optional] [default to '10s']
**directory** | **string** |  |
**api_secret** | **string** |  |
**endpoint** | **string** |  |
**login_id** | **string** | Username of the API Key holder |
**username** | **string** |  |
**password** | **string** |  |
**authorization_endpoint** | **string** |  |

[[Back to Model list]](../../README.md#models) [[Back to API list]](../../README.md#endpoints) [[Back to README]](../../README.md)
