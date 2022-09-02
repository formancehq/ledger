# TransactionData

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Timestamp** | Pointer to **time.Time** |  | [optional] 
**Postings** | [**[]Posting**](Posting.md) |  | 
**Reference** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 

## Methods

### NewTransactionData

`func NewTransactionData(postings []Posting, ) *TransactionData`

NewTransactionData instantiates a new TransactionData object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTransactionDataWithDefaults

`func NewTransactionDataWithDefaults() *TransactionData`

NewTransactionDataWithDefaults instantiates a new TransactionData object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTimestamp

`func (o *TransactionData) GetTimestamp() time.Time`

GetTimestamp returns the Timestamp field if non-nil, zero value otherwise.

### GetTimestampOk

`func (o *TransactionData) GetTimestampOk() (*time.Time, bool)`

GetTimestampOk returns a tuple with the Timestamp field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTimestamp

`func (o *TransactionData) SetTimestamp(v time.Time)`

SetTimestamp sets Timestamp field to given value.

### HasTimestamp

`func (o *TransactionData) HasTimestamp() bool`

HasTimestamp returns a boolean if a field has been set.

### GetPostings

`func (o *TransactionData) GetPostings() []Posting`

GetPostings returns the Postings field if non-nil, zero value otherwise.

### GetPostingsOk

`func (o *TransactionData) GetPostingsOk() (*[]Posting, bool)`

GetPostingsOk returns a tuple with the Postings field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostings

`func (o *TransactionData) SetPostings(v []Posting)`

SetPostings sets Postings field to given value.


### GetReference

`func (o *TransactionData) GetReference() string`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *TransactionData) GetReferenceOk() (*string, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *TransactionData) SetReference(v string)`

SetReference sets Reference field to given value.

### HasReference

`func (o *TransactionData) HasReference() bool`

HasReference returns a boolean if a field has been set.

### GetMetadata

`func (o *TransactionData) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *TransactionData) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *TransactionData) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *TransactionData) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### SetMetadataNil

`func (o *TransactionData) SetMetadataNil(b bool)`

 SetMetadataNil sets the value for Metadata to be an explicit nil

### UnsetMetadata
`func (o *TransactionData) UnsetMetadata()`

UnsetMetadata ensures that no value is present for Metadata, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


