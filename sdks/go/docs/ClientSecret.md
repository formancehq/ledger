# ClientSecret

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**LastDigits** | **string** |  | 
**Name** | **string** |  | 
**Id** | **string** |  | 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 

## Methods

### NewClientSecret

`func NewClientSecret(lastDigits string, name string, id string, ) *ClientSecret`

NewClientSecret instantiates a new ClientSecret object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewClientSecretWithDefaults

`func NewClientSecretWithDefaults() *ClientSecret`

NewClientSecretWithDefaults instantiates a new ClientSecret object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetLastDigits

`func (o *ClientSecret) GetLastDigits() string`

GetLastDigits returns the LastDigits field if non-nil, zero value otherwise.

### GetLastDigitsOk

`func (o *ClientSecret) GetLastDigitsOk() (*string, bool)`

GetLastDigitsOk returns a tuple with the LastDigits field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLastDigits

`func (o *ClientSecret) SetLastDigits(v string)`

SetLastDigits sets LastDigits field to given value.


### GetName

`func (o *ClientSecret) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ClientSecret) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ClientSecret) SetName(v string)`

SetName sets Name field to given value.


### GetId

`func (o *ClientSecret) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *ClientSecret) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *ClientSecret) SetId(v string)`

SetId sets Id field to given value.


### GetMetadata

`func (o *ClientSecret) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *ClientSecret) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *ClientSecret) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *ClientSecret) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


