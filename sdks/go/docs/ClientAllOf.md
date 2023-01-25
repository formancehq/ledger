# ClientAllOf

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** |  | 
**Scopes** | Pointer to **[]string** |  | [optional] 
**Secrets** | Pointer to [**[]ClientSecret**](ClientSecret.md) |  | [optional] 

## Methods

### NewClientAllOf

`func NewClientAllOf(id string, ) *ClientAllOf`

NewClientAllOf instantiates a new ClientAllOf object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewClientAllOfWithDefaults

`func NewClientAllOfWithDefaults() *ClientAllOf`

NewClientAllOfWithDefaults instantiates a new ClientAllOf object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *ClientAllOf) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *ClientAllOf) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *ClientAllOf) SetId(v string)`

SetId sets Id field to given value.


### GetScopes

`func (o *ClientAllOf) GetScopes() []string`

GetScopes returns the Scopes field if non-nil, zero value otherwise.

### GetScopesOk

`func (o *ClientAllOf) GetScopesOk() (*[]string, bool)`

GetScopesOk returns a tuple with the Scopes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetScopes

`func (o *ClientAllOf) SetScopes(v []string)`

SetScopes sets Scopes field to given value.

### HasScopes

`func (o *ClientAllOf) HasScopes() bool`

HasScopes returns a boolean if a field has been set.

### GetSecrets

`func (o *ClientAllOf) GetSecrets() []ClientSecret`

GetSecrets returns the Secrets field if non-nil, zero value otherwise.

### GetSecretsOk

`func (o *ClientAllOf) GetSecretsOk() (*[]ClientSecret, bool)`

GetSecretsOk returns a tuple with the Secrets field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSecrets

`func (o *ClientAllOf) SetSecrets(v []ClientSecret)`

SetSecrets sets Secrets field to given value.

### HasSecrets

`func (o *ClientAllOf) HasSecrets() bool`

HasSecrets returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


