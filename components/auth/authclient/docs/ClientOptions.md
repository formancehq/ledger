# ClientOptions

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Public** | Pointer to **interface{}** |  | [optional]
**RedirectUris** | Pointer to **interface{}** |  | [optional]
**Description** | Pointer to **interface{}** |  | [optional]
**Name** | **interface{}** |  |
**Trusted** | Pointer to **interface{}** |  | [optional]
**PostLogoutRedirectUris** | Pointer to **interface{}** |  | [optional]
**Metadata** | Pointer to  |  | [optional]

## Methods

### NewClientOptions

`func NewClientOptions(name interface{}, ) *ClientOptions`

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

`func (o *ClientOptions) GetPublic() interface{}`

GetPublic returns the Public field if non-nil, zero value otherwise.

### GetPublicOk

`func (o *ClientOptions) GetPublicOk() (*interface{}, bool)`

GetPublicOk returns a tuple with the Public field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPublic

`func (o *ClientOptions) SetPublic(v interface{})`

SetPublic sets Public field to given value.

### HasPublic

`func (o *ClientOptions) HasPublic() bool`

HasPublic returns a boolean if a field has been set.

### SetPublicNil

`func (o *ClientOptions) SetPublicNil(b bool)`

 SetPublicNil sets the value for Public to be an explicit nil

### UnsetPublic
`func (o *ClientOptions) UnsetPublic()`

UnsetPublic ensures that no value is present for Public, not even an explicit nil
### GetRedirectUris

`func (o *ClientOptions) GetRedirectUris() interface{}`

GetRedirectUris returns the RedirectUris field if non-nil, zero value otherwise.

### GetRedirectUrisOk

`func (o *ClientOptions) GetRedirectUrisOk() (*interface{}, bool)`

GetRedirectUrisOk returns a tuple with the RedirectUris field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRedirectUris

`func (o *ClientOptions) SetRedirectUris(v interface{})`

SetRedirectUris sets RedirectUris field to given value.

### HasRedirectUris

`func (o *ClientOptions) HasRedirectUris() bool`

HasRedirectUris returns a boolean if a field has been set.

### SetRedirectUrisNil

`func (o *ClientOptions) SetRedirectUrisNil(b bool)`

 SetRedirectUrisNil sets the value for RedirectUris to be an explicit nil

### UnsetRedirectUris
`func (o *ClientOptions) UnsetRedirectUris()`

UnsetRedirectUris ensures that no value is present for RedirectUris, not even an explicit nil
### GetDescription

`func (o *ClientOptions) GetDescription() interface{}`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *ClientOptions) GetDescriptionOk() (*interface{}, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *ClientOptions) SetDescription(v interface{})`

SetDescription sets Description field to given value.

### HasDescription

`func (o *ClientOptions) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### SetDescriptionNil

`func (o *ClientOptions) SetDescriptionNil(b bool)`

 SetDescriptionNil sets the value for Description to be an explicit nil

### UnsetDescription
`func (o *ClientOptions) UnsetDescription()`

UnsetDescription ensures that no value is present for Description, not even an explicit nil
### GetName

`func (o *ClientOptions) GetName() interface{}`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ClientOptions) GetNameOk() (*interface{}, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ClientOptions) SetName(v interface{})`

SetName sets Name field to given value.


### SetNameNil

`func (o *ClientOptions) SetNameNil(b bool)`

 SetNameNil sets the value for Name to be an explicit nil

### UnsetName
`func (o *ClientOptions) UnsetName()`

UnsetName ensures that no value is present for Name, not even an explicit nil
### GetTrusted

`func (o *ClientOptions) GetTrusted() interface{}`

GetTrusted returns the Trusted field if non-nil, zero value otherwise.

### GetTrustedOk

`func (o *ClientOptions) GetTrustedOk() (*interface{}, bool)`

GetTrustedOk returns a tuple with the Trusted field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTrusted

`func (o *ClientOptions) SetTrusted(v interface{})`

SetTrusted sets Trusted field to given value.

### HasTrusted

`func (o *ClientOptions) HasTrusted() bool`

HasTrusted returns a boolean if a field has been set.

### SetTrustedNil

`func (o *ClientOptions) SetTrustedNil(b bool)`

 SetTrustedNil sets the value for Trusted to be an explicit nil

### UnsetTrusted
`func (o *ClientOptions) UnsetTrusted()`

UnsetTrusted ensures that no value is present for Trusted, not even an explicit nil
### GetPostLogoutRedirectUris

`func (o *ClientOptions) GetPostLogoutRedirectUris() interface{}`

GetPostLogoutRedirectUris returns the PostLogoutRedirectUris field if non-nil, zero value otherwise.

### GetPostLogoutRedirectUrisOk

`func (o *ClientOptions) GetPostLogoutRedirectUrisOk() (*interface{}, bool)`

GetPostLogoutRedirectUrisOk returns a tuple with the PostLogoutRedirectUris field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostLogoutRedirectUris

`func (o *ClientOptions) SetPostLogoutRedirectUris(v interface{})`

SetPostLogoutRedirectUris sets PostLogoutRedirectUris field to given value.

### HasPostLogoutRedirectUris

`func (o *ClientOptions) HasPostLogoutRedirectUris() bool`

HasPostLogoutRedirectUris returns a boolean if a field has been set.

### SetPostLogoutRedirectUrisNil

`func (o *ClientOptions) SetPostLogoutRedirectUrisNil(b bool)`

 SetPostLogoutRedirectUrisNil sets the value for PostLogoutRedirectUris to be an explicit nil

### UnsetPostLogoutRedirectUris
`func (o *ClientOptions) UnsetPostLogoutRedirectUris()`

UnsetPostLogoutRedirectUris ensures that no value is present for PostLogoutRedirectUris, not even an explicit nil
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

### SetMetadataNil

`func (o *ClientOptions) SetMetadataNil(b bool)`

 SetMetadataNil sets the value for Metadata to be an explicit nil

### UnsetMetadata
`func (o *ClientOptions) UnsetMetadata()`

UnsetMetadata ensures that no value is present for Metadata, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
