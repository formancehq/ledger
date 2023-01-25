# PaymentsAccount

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** |  | 
**CreatedAt** | **time.Time** |  | 
**Provider** | [**Connector**](Connector.md) |  | 
**Reference** | **string** |  | 
**Type** | **string** |  | 

## Methods

### NewPaymentsAccount

`func NewPaymentsAccount(id string, createdAt time.Time, provider Connector, reference string, type_ string, ) *PaymentsAccount`

NewPaymentsAccount instantiates a new PaymentsAccount object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPaymentsAccountWithDefaults

`func NewPaymentsAccountWithDefaults() *PaymentsAccount`

NewPaymentsAccountWithDefaults instantiates a new PaymentsAccount object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *PaymentsAccount) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *PaymentsAccount) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *PaymentsAccount) SetId(v string)`

SetId sets Id field to given value.


### GetCreatedAt

`func (o *PaymentsAccount) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *PaymentsAccount) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *PaymentsAccount) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.


### GetProvider

`func (o *PaymentsAccount) GetProvider() Connector`

GetProvider returns the Provider field if non-nil, zero value otherwise.

### GetProviderOk

`func (o *PaymentsAccount) GetProviderOk() (*Connector, bool)`

GetProviderOk returns a tuple with the Provider field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProvider

`func (o *PaymentsAccount) SetProvider(v Connector)`

SetProvider sets Provider field to given value.


### GetReference

`func (o *PaymentsAccount) GetReference() string`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *PaymentsAccount) GetReferenceOk() (*string, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *PaymentsAccount) SetReference(v string)`

SetReference sets Reference field to given value.


### GetType

`func (o *PaymentsAccount) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *PaymentsAccount) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *PaymentsAccount) SetType(v string)`

SetType sets Type field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


