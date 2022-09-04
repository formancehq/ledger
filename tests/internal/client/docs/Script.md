# Script

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Reference** | Pointer to **string** | Reference to attach to the generated transaction | [optional] 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 
**Plain** | **string** |  | 
**Vars** | Pointer to **map[string]interface{}** |  | [optional] 

## Methods

### NewScript

`func NewScript(plain string, ) *Script`

NewScript instantiates a new Script object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewScriptWithDefaults

`func NewScriptWithDefaults() *Script`

NewScriptWithDefaults instantiates a new Script object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetReference

`func (o *Script) GetReference() string`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *Script) GetReferenceOk() (*string, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *Script) SetReference(v string)`

SetReference sets Reference field to given value.

### HasReference

`func (o *Script) HasReference() bool`

HasReference returns a boolean if a field has been set.

### GetMetadata

`func (o *Script) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Script) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Script) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Script) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### SetMetadataNil

`func (o *Script) SetMetadataNil(b bool)`

 SetMetadataNil sets the value for Metadata to be an explicit nil

### UnsetMetadata
`func (o *Script) UnsetMetadata()`

UnsetMetadata ensures that no value is present for Metadata, not even an explicit nil
### GetPlain

`func (o *Script) GetPlain() string`

GetPlain returns the Plain field if non-nil, zero value otherwise.

### GetPlainOk

`func (o *Script) GetPlainOk() (*string, bool)`

GetPlainOk returns a tuple with the Plain field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPlain

`func (o *Script) SetPlain(v string)`

SetPlain sets Plain field to given value.


### GetVars

`func (o *Script) GetVars() map[string]interface{}`

GetVars returns the Vars field if non-nil, zero value otherwise.

### GetVarsOk

`func (o *Script) GetVarsOk() (*map[string]interface{}, bool)`

GetVarsOk returns a tuple with the Vars field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVars

`func (o *Script) SetVars(v map[string]interface{})`

SetVars sets Vars field to given value.

### HasVars

`func (o *Script) HasVars() bool`

HasVars returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


