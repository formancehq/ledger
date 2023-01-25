# ConnectorBaseInfo

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Provider** | Pointer to **string** |  | [optional]
**Disabled** | Pointer to **bool** |  | [optional]

## Methods

### NewConnectorBaseInfo

`func NewConnectorBaseInfo() *ConnectorBaseInfo`

NewConnectorBaseInfo instantiates a new ConnectorBaseInfo object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewConnectorBaseInfoWithDefaults

`func NewConnectorBaseInfoWithDefaults() *ConnectorBaseInfo`

NewConnectorBaseInfoWithDefaults instantiates a new ConnectorBaseInfo object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetProvider

`func (o *ConnectorBaseInfo) GetProvider() string`

GetProvider returns the Provider field if non-nil, zero value otherwise.

### GetProviderOk

`func (o *ConnectorBaseInfo) GetProviderOk() (*string, bool)`

GetProviderOk returns a tuple with the Provider field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProvider

`func (o *ConnectorBaseInfo) SetProvider(v string)`

SetProvider sets Provider field to given value.

### HasProvider

`func (o *ConnectorBaseInfo) HasProvider() bool`

HasProvider returns a boolean if a field has been set.

### GetDisabled

`func (o *ConnectorBaseInfo) GetDisabled() bool`

GetDisabled returns the Disabled field if non-nil, zero value otherwise.

### GetDisabledOk

`func (o *ConnectorBaseInfo) GetDisabledOk() (*bool, bool)`

GetDisabledOk returns a tuple with the Disabled field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDisabled

`func (o *ConnectorBaseInfo) SetDisabled(v bool)`

SetDisabled sets Disabled field to given value.

### HasDisabled

`func (o *ConnectorBaseInfo) HasDisabled() bool`

HasDisabled returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
