# ClientOptions

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

## Methods

### NewClientOptions

`func NewClientOptions(name string, ) *ClientOptions`

NewClientOptions instantiates a new ClientOptions object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewClientOptionsWithDefaults

`func NewClientOptionsWithDefaults() *ClientOptions`

NewClientOptionsWithDefaults instantiates a new ClientOptions object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPublic

`func (o *ClientOptions) GetPublic() bool`

GetPublic returns the Public field if non-nil, zero value otherwise.

### GetPublicOk

`func (o *ClientOptions) GetPublicOk() (*bool, bool)`

GetPublicOk returns a tuple with the Public field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPublic

`func (o *ClientOptions) SetPublic(v bool)`

SetPublic sets Public field to given value.

### HasPublic

`func (o *ClientOptions) HasPublic() bool`

HasPublic returns a boolean if a field has been set.

### GetRedirectUris

`func (o *ClientOptions) GetRedirectUris() []string`

GetRedirectUris returns the RedirectUris field if non-nil, zero value otherwise.

### GetRedirectUrisOk

`func (o *ClientOptions) GetRedirectUrisOk() (*[]string, bool)`

GetRedirectUrisOk returns a tuple with the RedirectUris field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRedirectUris

`func (o *ClientOptions) SetRedirectUris(v []string)`

SetRedirectUris sets RedirectUris field to given value.

### HasRedirectUris

`func (o *ClientOptions) HasRedirectUris() bool`

HasRedirectUris returns a boolean if a field has been set.

### GetDescription

`func (o *ClientOptions) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *ClientOptions) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *ClientOptions) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *ClientOptions) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetName

`func (o *ClientOptions) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ClientOptions) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ClientOptions) SetName(v string)`

SetName sets Name field to given value.


### GetTrusted

`func (o *ClientOptions) GetTrusted() bool`

GetTrusted returns the Trusted field if non-nil, zero value otherwise.

### GetTrustedOk

`func (o *ClientOptions) GetTrustedOk() (*bool, bool)`

GetTrustedOk returns a tuple with the Trusted field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTrusted

`func (o *ClientOptions) SetTrusted(v bool)`

SetTrusted sets Trusted field to given value.

### HasTrusted

`func (o *ClientOptions) HasTrusted() bool`

HasTrusted returns a boolean if a field has been set.

### GetPostLogoutRedirectUris

`func (o *ClientOptions) GetPostLogoutRedirectUris() []string`

GetPostLogoutRedirectUris returns the PostLogoutRedirectUris field if non-nil, zero value otherwise.

### GetPostLogoutRedirectUrisOk

`func (o *ClientOptions) GetPostLogoutRedirectUrisOk() (*[]string, bool)`

GetPostLogoutRedirectUrisOk returns a tuple with the PostLogoutRedirectUris field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostLogoutRedirectUris

`func (o *ClientOptions) SetPostLogoutRedirectUris(v []string)`

SetPostLogoutRedirectUris sets PostLogoutRedirectUris field to given value.

### HasPostLogoutRedirectUris

`func (o *ClientOptions) HasPostLogoutRedirectUris() bool`

HasPostLogoutRedirectUris returns a boolean if a field has been set.

### GetMetadata

`func (o *ClientOptions) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *ClientOptions) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *ClientOptions) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *ClientOptions) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


