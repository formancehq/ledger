# ConfigInfo

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Config** | [**Config**](Config.md) |  | 
**Server** | **string** |  | 
**Version** | **string** |  | 

## Methods

### NewConfigInfo

`func NewConfigInfo(config Config, server string, version string, ) *ConfigInfo`

NewConfigInfo instantiates a new ConfigInfo object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewConfigInfoWithDefaults

`func NewConfigInfoWithDefaults() *ConfigInfo`

NewConfigInfoWithDefaults instantiates a new ConfigInfo object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetConfig

`func (o *ConfigInfo) GetConfig() Config`

GetConfig returns the Config field if non-nil, zero value otherwise.

### GetConfigOk

`func (o *ConfigInfo) GetConfigOk() (*Config, bool)`

GetConfigOk returns a tuple with the Config field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConfig

`func (o *ConfigInfo) SetConfig(v Config)`

SetConfig sets Config field to given value.


### GetServer

`func (o *ConfigInfo) GetServer() string`

GetServer returns the Server field if non-nil, zero value otherwise.

### GetServerOk

`func (o *ConfigInfo) GetServerOk() (*string, bool)`

GetServerOk returns a tuple with the Server field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetServer

`func (o *ConfigInfo) SetServer(v string)`

SetServer sets Server field to given value.


### GetVersion

`func (o *ConfigInfo) GetVersion() string`

GetVersion returns the Version field if non-nil, zero value otherwise.

### GetVersionOk

`func (o *ConfigInfo) GetVersionOk() (*string, bool)`

GetVersionOk returns a tuple with the Version field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersion

`func (o *ConfigInfo) SetVersion(v string)`

SetVersion sets Version field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


