# DummyPayConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**FilePollingPeriod** | Pointer to **string** | The frequency at which the connector will try to fetch new payment objects from the directory | [optional] [default to "10s"]
**FileGenerationPeriod** | Pointer to **string** | The frequency at which the connector will create new payment objects in the directory | [optional] [default to "10s"]
**Directory** | **string** |  | 

## Methods

### NewDummyPayConfig

`func NewDummyPayConfig(directory string, ) *DummyPayConfig`

NewDummyPayConfig instantiates a new DummyPayConfig object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDummyPayConfigWithDefaults

`func NewDummyPayConfigWithDefaults() *DummyPayConfig`

NewDummyPayConfigWithDefaults instantiates a new DummyPayConfig object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetFilePollingPeriod

`func (o *DummyPayConfig) GetFilePollingPeriod() string`

GetFilePollingPeriod returns the FilePollingPeriod field if non-nil, zero value otherwise.

### GetFilePollingPeriodOk

`func (o *DummyPayConfig) GetFilePollingPeriodOk() (*string, bool)`

GetFilePollingPeriodOk returns a tuple with the FilePollingPeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFilePollingPeriod

`func (o *DummyPayConfig) SetFilePollingPeriod(v string)`

SetFilePollingPeriod sets FilePollingPeriod field to given value.

### HasFilePollingPeriod

`func (o *DummyPayConfig) HasFilePollingPeriod() bool`

HasFilePollingPeriod returns a boolean if a field has been set.

### GetFileGenerationPeriod

`func (o *DummyPayConfig) GetFileGenerationPeriod() string`

GetFileGenerationPeriod returns the FileGenerationPeriod field if non-nil, zero value otherwise.

### GetFileGenerationPeriodOk

`func (o *DummyPayConfig) GetFileGenerationPeriodOk() (*string, bool)`

GetFileGenerationPeriodOk returns a tuple with the FileGenerationPeriod field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFileGenerationPeriod

`func (o *DummyPayConfig) SetFileGenerationPeriod(v string)`

SetFileGenerationPeriod sets FileGenerationPeriod field to given value.

### HasFileGenerationPeriod

`func (o *DummyPayConfig) HasFileGenerationPeriod() bool`

HasFileGenerationPeriod returns a boolean if a field has been set.

### GetDirectory

`func (o *DummyPayConfig) GetDirectory() string`

GetDirectory returns the Directory field if non-nil, zero value otherwise.

### GetDirectoryOk

`func (o *DummyPayConfig) GetDirectoryOk() (*string, bool)`

GetDirectoryOk returns a tuple with the Directory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDirectory

`func (o *DummyPayConfig) SetDirectory(v string)`

SetDirectory sets Directory field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


