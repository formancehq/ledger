# Client

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Public** | Pointer to **bool** |  | [optional] 
**RedirectUris** | Pointer to **[]string** |  | [optional] 
**Description** | Pointer to **string** |  | [optional] 
**Name** | **string** |  | 
**Trusted** | Pointer to **bool** |  | [optional] 
**PostLogoutRedirectUris** | Pointer to **[]string** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 
**Id** | **string** |  | 
**Scopes** | Pointer to **[]string** |  | [optional] 
**Secrets** | Pointer to [**[]ClientSecret**](ClientSecret.md) |  | [optional] 

## Methods

### NewClient

`func NewClient(name string, id string, ) *Client`

NewClient instantiates a new Client object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewClientWithDefaults

`func NewClientWithDefaults() *Client`

NewClientWithDefaults instantiates a new Client object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPublic

`func (o *Client) GetPublic() bool`

GetPublic returns the Public field if non-nil, zero value otherwise.

### GetPublicOk

`func (o *Client) GetPublicOk() (*bool, bool)`

GetPublicOk returns a tuple with the Public field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPublic

`func (o *Client) SetPublic(v bool)`

SetPublic sets Public field to given value.

### HasPublic

`func (o *Client) HasPublic() bool`

HasPublic returns a boolean if a field has been set.

### GetRedirectUris

`func (o *Client) GetRedirectUris() []string`

GetRedirectUris returns the RedirectUris field if non-nil, zero value otherwise.

### GetRedirectUrisOk

`func (o *Client) GetRedirectUrisOk() (*[]string, bool)`

GetRedirectUrisOk returns a tuple with the RedirectUris field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRedirectUris

`func (o *Client) SetRedirectUris(v []string)`

SetRedirectUris sets RedirectUris field to given value.

### HasRedirectUris

`func (o *Client) HasRedirectUris() bool`

HasRedirectUris returns a boolean if a field has been set.

### GetDescription

`func (o *Client) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *Client) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *Client) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *Client) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetName

`func (o *Client) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Client) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Client) SetName(v string)`

SetName sets Name field to given value.


### GetTrusted

`func (o *Client) GetTrusted() bool`

GetTrusted returns the Trusted field if non-nil, zero value otherwise.

### GetTrustedOk

`func (o *Client) GetTrustedOk() (*bool, bool)`

GetTrustedOk returns a tuple with the Trusted field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTrusted

`func (o *Client) SetTrusted(v bool)`

SetTrusted sets Trusted field to given value.

### HasTrusted

`func (o *Client) HasTrusted() bool`

HasTrusted returns a boolean if a field has been set.

### GetPostLogoutRedirectUris

`func (o *Client) GetPostLogoutRedirectUris() []string`

GetPostLogoutRedirectUris returns the PostLogoutRedirectUris field if non-nil, zero value otherwise.

### GetPostLogoutRedirectUrisOk

`func (o *Client) GetPostLogoutRedirectUrisOk() (*[]string, bool)`

GetPostLogoutRedirectUrisOk returns a tuple with the PostLogoutRedirectUris field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostLogoutRedirectUris

`func (o *Client) SetPostLogoutRedirectUris(v []string)`

SetPostLogoutRedirectUris sets PostLogoutRedirectUris field to given value.

### HasPostLogoutRedirectUris

`func (o *Client) HasPostLogoutRedirectUris() bool`

HasPostLogoutRedirectUris returns a boolean if a field has been set.

### GetMetadata

`func (o *Client) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Client) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Client) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Client) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetId

`func (o *Client) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Client) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Client) SetId(v string)`

SetId sets Id field to given value.


### GetScopes

`func (o *Client) GetScopes() []string`

GetScopes returns the Scopes field if non-nil, zero value otherwise.

### GetScopesOk

`func (o *Client) GetScopesOk() (*[]string, bool)`

GetScopesOk returns a tuple with the Scopes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetScopes

`func (o *Client) SetScopes(v []string)`

SetScopes sets Scopes field to given value.

### HasScopes

`func (o *Client) HasScopes() bool`

HasScopes returns a boolean if a field has been set.

### GetSecrets

`func (o *Client) GetSecrets() []ClientSecret`

GetSecrets returns the Secrets field if non-nil, zero value otherwise.

### GetSecretsOk

`func (o *Client) GetSecretsOk() (*[]ClientSecret, bool)`

GetSecretsOk returns a tuple with the Secrets field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSecrets

`func (o *Client) SetSecrets(v []ClientSecret)`

SetSecrets sets Secrets field to given value.

### HasSecrets

`func (o *Client) HasSecrets() bool`

HasSecrets returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


