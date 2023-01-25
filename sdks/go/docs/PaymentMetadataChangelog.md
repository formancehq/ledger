# PaymentMetadataChangelog

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Timestamp** | **time.Time** |  | 
**Value** | **string** |  | 

## Methods

### NewPaymentMetadataChangelog

`func NewPaymentMetadataChangelog(timestamp time.Time, value string, ) *PaymentMetadataChangelog`

NewPaymentMetadataChangelog instantiates a new PaymentMetadataChangelog object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPaymentMetadataChangelogWithDefaults

`func NewPaymentMetadataChangelogWithDefaults() *PaymentMetadataChangelog`

NewPaymentMetadataChangelogWithDefaults instantiates a new PaymentMetadataChangelog object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTimestamp

`func (o *PaymentMetadataChangelog) GetTimestamp() time.Time`

GetTimestamp returns the Timestamp field if non-nil, zero value otherwise.

### GetTimestampOk

`func (o *PaymentMetadataChangelog) GetTimestampOk() (*time.Time, bool)`

GetTimestampOk returns a tuple with the Timestamp field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTimestamp

`func (o *PaymentMetadataChangelog) SetTimestamp(v time.Time)`

SetTimestamp sets Timestamp field to given value.


### GetValue

`func (o *PaymentMetadataChangelog) GetValue() string`

GetValue returns the Value field if non-nil, zero value otherwise.

### GetValueOk

`func (o *PaymentMetadataChangelog) GetValueOk() (*string, bool)`

GetValueOk returns a tuple with the Value field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetValue

`func (o *PaymentMetadataChangelog) SetValue(v string)`

SetValue sets Value field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


