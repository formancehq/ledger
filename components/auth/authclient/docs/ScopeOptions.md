# ScopeOptions

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Label** | **interface{}** |  |
**Metadata** | Pointer to  |  | [optional]

## Methods

### NewScopeOptions

`func NewScopeOptions(label interface{}, ) *ScopeOptions`

NewScopeOptions instantiates a new ScopeOptions object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewScopeOptionsWithDefaults

`func NewScopeOptionsWithDefaults() *ScopeOptions`

NewScopeOptionsWithDefaults instantiates a new ScopeOptions object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetLabel

`func (o *ScopeOptions) GetLabel() interface{}`

GetLabel returns the Label field if non-nil, zero value otherwise.

### GetLabelOk

`func (o *ScopeOptions) GetLabelOk() (*interface{}, bool)`

GetLabelOk returns a tuple with the Label field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLabel

`func (o *ScopeOptions) SetLabel(v interface{})`

SetLabel sets Label field to given value.


### SetLabelNil

`func (o *ScopeOptions) SetLabelNil(b bool)`

 SetLabelNil sets the value for Label to be an explicit nil

### UnsetLabel
`func (o *ScopeOptions) UnsetLabel()`

UnsetLabel ensures that no value is present for Label, not even an explicit nil
### GetMetadata

`func (o *ScopeOptions) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *ScopeOptions) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *ScopeOptions) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *ScopeOptions) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### SetMetadataNil

`func (o *ScopeOptions) SetMetadataNil(b bool)`

 SetMetadataNil sets the value for Metadata to be an explicit nil

### UnsetMetadata
`func (o *ScopeOptions) UnsetMetadata()`

UnsetMetadata ensures that no value is present for Metadata, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
