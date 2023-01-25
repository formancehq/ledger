# PaymentMetadata

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | **string** |  | 
**Value** | **string** |  | 
**Changelog** | Pointer to [**PaymentMetadataChangelog**](PaymentMetadataChangelog.md) |  | [optional] 

## Methods

### NewPaymentMetadata

`func NewPaymentMetadata(key string, value string, ) *PaymentMetadata`

NewPaymentMetadata instantiates a new PaymentMetadata object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPaymentMetadataWithDefaults

`func NewPaymentMetadataWithDefaults() *PaymentMetadata`

NewPaymentMetadataWithDefaults instantiates a new PaymentMetadata object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *PaymentMetadata) GetKey() string`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *PaymentMetadata) GetKeyOk() (*string, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *PaymentMetadata) SetKey(v string)`

SetKey sets Key field to given value.


### GetValue

`func (o *PaymentMetadata) GetValue() string`

GetValue returns the Value field if non-nil, zero value otherwise.

### GetValueOk

`func (o *PaymentMetadata) GetValueOk() (*string, bool)`

GetValueOk returns a tuple with the Value field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetValue

`func (o *PaymentMetadata) SetValue(v string)`

SetValue sets Value field to given value.


### GetChangelog

`func (o *PaymentMetadata) GetChangelog() PaymentMetadataChangelog`

GetChangelog returns the Changelog field if non-nil, zero value otherwise.

### GetChangelogOk

`func (o *PaymentMetadata) GetChangelogOk() (*PaymentMetadataChangelog, bool)`

GetChangelogOk returns a tuple with the Changelog field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetChangelog

`func (o *PaymentMetadata) SetChangelog(v PaymentMetadataChangelog)`

SetChangelog sets Changelog field to given value.

### HasChangelog

`func (o *PaymentMetadata) HasChangelog() bool`

HasChangelog returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


