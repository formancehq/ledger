# ConfigsResponseCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | **bool** |  | 
**Data** | [**[]WebhooksConfig**](WebhooksConfig.md) |  | 

## Methods

### NewConfigsResponseCursor

`func NewConfigsResponseCursor(hasMore bool, data []WebhooksConfig, ) *ConfigsResponseCursor`

NewConfigsResponseCursor instantiates a new ConfigsResponseCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewConfigsResponseCursorWithDefaults

`func NewConfigsResponseCursorWithDefaults() *ConfigsResponseCursor`

NewConfigsResponseCursorWithDefaults instantiates a new ConfigsResponseCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *ConfigsResponseCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *ConfigsResponseCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *ConfigsResponseCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.


### GetData

`func (o *ConfigsResponseCursor) GetData() []WebhooksConfig`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *ConfigsResponseCursor) GetDataOk() (*[]WebhooksConfig, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *ConfigsResponseCursor) SetData(v []WebhooksConfig)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


