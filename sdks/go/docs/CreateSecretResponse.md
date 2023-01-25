# CreateSecretResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | Pointer to [**Secret**](Secret.md) |  | [optional] 

## Methods

### NewCreateSecretResponse

`func NewCreateSecretResponse() *CreateSecretResponse`

NewCreateSecretResponse instantiates a new CreateSecretResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCreateSecretResponseWithDefaults

`func NewCreateSecretResponseWithDefaults() *CreateSecretResponse`

NewCreateSecretResponseWithDefaults instantiates a new CreateSecretResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *CreateSecretResponse) GetData() Secret`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *CreateSecretResponse) GetDataOk() (*Secret, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *CreateSecretResponse) SetData(v Secret)`

SetData sets Data field to given value.

### HasData

`func (o *CreateSecretResponse) HasData() bool`

HasData returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


