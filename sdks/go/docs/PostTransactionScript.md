# PostTransactionScript

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Plain** | **string** |  | 
**Vars** | Pointer to **map[string]interface{}** |  | [optional] 

## Methods

### NewPostTransactionScript

`func NewPostTransactionScript(plain string, ) *PostTransactionScript`

NewPostTransactionScript instantiates a new PostTransactionScript object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPostTransactionScriptWithDefaults

`func NewPostTransactionScriptWithDefaults() *PostTransactionScript`

NewPostTransactionScriptWithDefaults instantiates a new PostTransactionScript object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPlain

`func (o *PostTransactionScript) GetPlain() string`

GetPlain returns the Plain field if non-nil, zero value otherwise.

### GetPlainOk

`func (o *PostTransactionScript) GetPlainOk() (*string, bool)`

GetPlainOk returns a tuple with the Plain field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPlain

`func (o *PostTransactionScript) SetPlain(v string)`

SetPlain sets Plain field to given value.


### GetVars

`func (o *PostTransactionScript) GetVars() map[string]interface{}`

GetVars returns the Vars field if non-nil, zero value otherwise.

### GetVarsOk

`func (o *PostTransactionScript) GetVarsOk() (*map[string]interface{}, bool)`

GetVarsOk returns a tuple with the Vars field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVars

`func (o *PostTransactionScript) SetVars(v map[string]interface{})`

SetVars sets Vars field to given value.

### HasVars

`func (o *PostTransactionScript) HasVars() bool`

HasVars returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


