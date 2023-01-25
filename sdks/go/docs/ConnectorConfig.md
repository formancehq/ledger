# ConnectorConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PollingPeriod** | Pointer to **string** | The frequency at which the connector will fetch transactions | [optional] 
**ApiKey** | **string** |  | 
**PageSize** | Pointer to **int64** | Number of BalanceTransaction to fetch at each polling interval.  | [optional] [default to 10]
**FilePollingPeriod** | Pointer to **string** | The frequency at which the connector will try to fetch new payment objects from the directory | [optional] [default to "10s"]
**FileGenerationPeriod** | Pointer to **string** | The frequency at which the connector will create new payment objects in the directory | [optional] [default to "10s"]
**Directory** | **string** |  | 
**ApiSecret** | **string** |  | 
**Endpoint** | **string** |  | 
**LoginID** | **string** | Username of the API Key holder | 
**Username** | **string** |  | 
**Password** | **string** |  | 
**AuthorizationEndpoint** | **string** |  | 

## Methods

### NewConnectorConfig

`func NewConnectorConfig(apiKey string, directory string, apiSecret string, endpoint string, loginID string, username string, password string, authorizationEndpoint string, ) *ConnectorConfig`

NewConnectorConfig instantiates a new ConnectorConfig object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewConnectorConfigWithDefaults

`func NewConnectorConfigWithDefaults() *ConnectorConfig`

NewConnectorConfigWithDefaults instantiates a new ConnectorConfig object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPollingPeriod

`func (o *ConnectorConfig) GetPollingPeriod() string`

GetPollingPeriod returns the PollingPeriod field if non-nil, zero value otherwise.

### GetPollingPeriodOk

`func (o *ConnectorConfig) GetPollingPeriodOk() (*string, bool)`

GetPollingPeriodOk returns a tuple with the PollingPeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPollingPeriod

`func (o *ConnectorConfig) SetPollingPeriod(v string)`

SetPollingPeriod sets PollingPeriod field to given value.

### HasPollingPeriod

`func (o *ConnectorConfig) HasPollingPeriod() bool`

HasPollingPeriod returns a boolean if a field has been set.

### GetApiKey

`func (o *ConnectorConfig) GetApiKey() string`

GetApiKey returns the ApiKey field if non-nil, zero value otherwise.

### GetApiKeyOk

`func (o *ConnectorConfig) GetApiKeyOk() (*string, bool)`

GetApiKeyOk returns a tuple with the ApiKey field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetApiKey

`func (o *ConnectorConfig) SetApiKey(v string)`

SetApiKey sets ApiKey field to given value.


### GetPageSize

`func (o *ConnectorConfig) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *ConnectorConfig) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *ConnectorConfig) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.

### HasPageSize

`func (o *ConnectorConfig) HasPageSize() bool`

HasPageSize returns a boolean if a field has been set.

### GetFilePollingPeriod

`func (o *ConnectorConfig) GetFilePollingPeriod() string`

GetFilePollingPeriod returns the FilePollingPeriod field if non-nil, zero value otherwise.

### GetFilePollingPeriodOk

`func (o *ConnectorConfig) GetFilePollingPeriodOk() (*string, bool)`

GetFilePollingPeriodOk returns a tuple with the FilePollingPeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFilePollingPeriod

`func (o *ConnectorConfig) SetFilePollingPeriod(v string)`

SetFilePollingPeriod sets FilePollingPeriod field to given value.

### HasFilePollingPeriod

`func (o *ConnectorConfig) HasFilePollingPeriod() bool`

HasFilePollingPeriod returns a boolean if a field has been set.

### GetFileGenerationPeriod

`func (o *ConnectorConfig) GetFileGenerationPeriod() string`

GetFileGenerationPeriod returns the FileGenerationPeriod field if non-nil, zero value otherwise.

### GetFileGenerationPeriodOk

`func (o *ConnectorConfig) GetFileGenerationPeriodOk() (*string, bool)`

GetFileGenerationPeriodOk returns a tuple with the FileGenerationPeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFileGenerationPeriod

`func (o *ConnectorConfig) SetFileGenerationPeriod(v string)`

SetFileGenerationPeriod sets FileGenerationPeriod field to given value.

### HasFileGenerationPeriod

`func (o *ConnectorConfig) HasFileGenerationPeriod() bool`

HasFileGenerationPeriod returns a boolean if a field has been set.

### GetDirectory

`func (o *ConnectorConfig) GetDirectory() string`

GetDirectory returns the Directory field if non-nil, zero value otherwise.

### GetDirectoryOk

`func (o *ConnectorConfig) GetDirectoryOk() (*string, bool)`

GetDirectoryOk returns a tuple with the Directory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDirectory

`func (o *ConnectorConfig) SetDirectory(v string)`

SetDirectory sets Directory field to given value.


### GetApiSecret

`func (o *ConnectorConfig) GetApiSecret() string`

GetApiSecret returns the ApiSecret field if non-nil, zero value otherwise.

### GetApiSecretOk

`func (o *ConnectorConfig) GetApiSecretOk() (*string, bool)`

GetApiSecretOk returns a tuple with the ApiSecret field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetApiSecret

`func (o *ConnectorConfig) SetApiSecret(v string)`

SetApiSecret sets ApiSecret field to given value.


### GetEndpoint

`func (o *ConnectorConfig) GetEndpoint() string`

GetEndpoint returns the Endpoint field if non-nil, zero value otherwise.

### GetEndpointOk

`func (o *ConnectorConfig) GetEndpointOk() (*string, bool)`

GetEndpointOk returns a tuple with the Endpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEndpoint

`func (o *ConnectorConfig) SetEndpoint(v string)`

SetEndpoint sets Endpoint field to given value.


### GetLoginID

`func (o *ConnectorConfig) GetLoginID() string`

GetLoginID returns the LoginID field if non-nil, zero value otherwise.

### GetLoginIDOk

`func (o *ConnectorConfig) GetLoginIDOk() (*string, bool)`

GetLoginIDOk returns a tuple with the LoginID field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLoginID

`func (o *ConnectorConfig) SetLoginID(v string)`

SetLoginID sets LoginID field to given value.


### GetUsername

`func (o *ConnectorConfig) GetUsername() string`

GetUsername returns the Username field if non-nil, zero value otherwise.

### GetUsernameOk

`func (o *ConnectorConfig) GetUsernameOk() (*string, bool)`

GetUsernameOk returns a tuple with the Username field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUsername

`func (o *ConnectorConfig) SetUsername(v string)`

SetUsername sets Username field to given value.


### GetPassword

`func (o *ConnectorConfig) GetPassword() string`

GetPassword returns the Password field if non-nil, zero value otherwise.

### GetPasswordOk

`func (o *ConnectorConfig) GetPasswordOk() (*string, bool)`

GetPasswordOk returns a tuple with the Password field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassword

`func (o *ConnectorConfig) SetPassword(v string)`

SetPassword sets Password field to given value.


### GetAuthorizationEndpoint

`func (o *ConnectorConfig) GetAuthorizationEndpoint() string`

GetAuthorizationEndpoint returns the AuthorizationEndpoint field if non-nil, zero value otherwise.

### GetAuthorizationEndpointOk

`func (o *ConnectorConfig) GetAuthorizationEndpointOk() (*string, bool)`

GetAuthorizationEndpointOk returns a tuple with the AuthorizationEndpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthorizationEndpoint

`func (o *ConnectorConfig) SetAuthorizationEndpoint(v string)`

SetAuthorizationEndpoint sets AuthorizationEndpoint field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


