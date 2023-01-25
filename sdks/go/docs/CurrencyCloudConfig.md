# CurrencyCloudConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ApiKey** | **string** |  | 
**LoginID** | **string** | Username of the API Key holder | 
**PollingPeriod** | Pointer to **string** | The frequency at which the connector will fetch transactions | [optional] 
**Endpoint** | Pointer to **string** | The endpoint to use for the API. Defaults to https://devapi.currencycloud.com | [optional] 

## Methods

### NewCurrencyCloudConfig

`func NewCurrencyCloudConfig(apiKey string, loginID string, ) *CurrencyCloudConfig`

NewCurrencyCloudConfig instantiates a new CurrencyCloudConfig object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCurrencyCloudConfigWithDefaults

`func NewCurrencyCloudConfigWithDefaults() *CurrencyCloudConfig`

NewCurrencyCloudConfigWithDefaults instantiates a new CurrencyCloudConfig object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetApiKey

`func (o *CurrencyCloudConfig) GetApiKey() string`

GetApiKey returns the ApiKey field if non-nil, zero value otherwise.

### GetApiKeyOk

`func (o *CurrencyCloudConfig) GetApiKeyOk() (*string, bool)`

GetApiKeyOk returns a tuple with the ApiKey field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetApiKey

`func (o *CurrencyCloudConfig) SetApiKey(v string)`

SetApiKey sets ApiKey field to given value.


### GetLoginID

`func (o *CurrencyCloudConfig) GetLoginID() string`

GetLoginID returns the LoginID field if non-nil, zero value otherwise.

### GetLoginIDOk

`func (o *CurrencyCloudConfig) GetLoginIDOk() (*string, bool)`

GetLoginIDOk returns a tuple with the LoginID field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLoginID

`func (o *CurrencyCloudConfig) SetLoginID(v string)`

SetLoginID sets LoginID field to given value.


### GetPollingPeriod

`func (o *CurrencyCloudConfig) GetPollingPeriod() string`

GetPollingPeriod returns the PollingPeriod field if non-nil, zero value otherwise.

### GetPollingPeriodOk

`func (o *CurrencyCloudConfig) GetPollingPeriodOk() (*string, bool)`

GetPollingPeriodOk returns a tuple with the PollingPeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPollingPeriod

`func (o *CurrencyCloudConfig) SetPollingPeriod(v string)`

SetPollingPeriod sets PollingPeriod field to given value.

### HasPollingPeriod

`func (o *CurrencyCloudConfig) HasPollingPeriod() bool`

HasPollingPeriod returns a boolean if a field has been set.

### GetEndpoint

`func (o *CurrencyCloudConfig) GetEndpoint() string`

GetEndpoint returns the Endpoint field if non-nil, zero value otherwise.

### GetEndpointOk

`func (o *CurrencyCloudConfig) GetEndpointOk() (*string, bool)`

GetEndpointOk returns a tuple with the Endpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEndpoint

`func (o *CurrencyCloudConfig) SetEndpoint(v string)`

SetEndpoint sets Endpoint field to given value.

### HasEndpoint

`func (o *CurrencyCloudConfig) HasEndpoint() bool`

HasEndpoint returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


