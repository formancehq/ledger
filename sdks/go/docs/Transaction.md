# Transaction

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Timestamp** | **time.Time** |  | 
**Postings** | [**[]Posting**](Posting.md) |  | 
**Reference** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 
**Txid** | **int64** |  | 
**PreCommitVolumes** | Pointer to [**map[string]map[string]Volume**](map.md) |  | [optional] 
**PostCommitVolumes** | Pointer to [**map[string]map[string]Volume**](map.md) |  | [optional] 

## Methods

### NewTransaction

`func NewTransaction(timestamp time.Time, postings []Posting, txid int64, ) *Transaction`

NewTransaction instantiates a new Transaction object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTransactionWithDefaults

`func NewTransactionWithDefaults() *Transaction`

NewTransactionWithDefaults instantiates a new Transaction object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTimestamp

`func (o *Transaction) GetTimestamp() time.Time`

GetTimestamp returns the Timestamp field if non-nil, zero value otherwise.

### GetTimestampOk

`func (o *Transaction) GetTimestampOk() (*time.Time, bool)`

GetTimestampOk returns a tuple with the Timestamp field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTimestamp

`func (o *Transaction) SetTimestamp(v time.Time)`

SetTimestamp sets Timestamp field to given value.


### GetPostings

`func (o *Transaction) GetPostings() []Posting`

GetPostings returns the Postings field if non-nil, zero value otherwise.

### GetPostingsOk

`func (o *Transaction) GetPostingsOk() (*[]Posting, bool)`

GetPostingsOk returns a tuple with the Postings field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostings

`func (o *Transaction) SetPostings(v []Posting)`

SetPostings sets Postings field to given value.


### GetReference

`func (o *Transaction) GetReference() string`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *Transaction) GetReferenceOk() (*string, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *Transaction) SetReference(v string)`

SetReference sets Reference field to given value.

### HasReference

`func (o *Transaction) HasReference() bool`

HasReference returns a boolean if a field has been set.

### GetMetadata

`func (o *Transaction) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Transaction) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Transaction) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Transaction) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### SetMetadataNil

`func (o *Transaction) SetMetadataNil(b bool)`

 SetMetadataNil sets the value for Metadata to be an explicit nil

### UnsetMetadata
`func (o *Transaction) UnsetMetadata()`

UnsetMetadata ensures that no value is present for Metadata, not even an explicit nil
### GetTxid

`func (o *Transaction) GetTxid() int64`

GetTxid returns the Txid field if non-nil, zero value otherwise.

### GetTxidOk

`func (o *Transaction) GetTxidOk() (*int64, bool)`

GetTxidOk returns a tuple with the Txid field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTxid

`func (o *Transaction) SetTxid(v int64)`

SetTxid sets Txid field to given value.


### GetPreCommitVolumes

`func (o *Transaction) GetPreCommitVolumes() map[string]map[string]Volume`

GetPreCommitVolumes returns the PreCommitVolumes field if non-nil, zero value otherwise.

### GetPreCommitVolumesOk

`func (o *Transaction) GetPreCommitVolumesOk() (*map[string]map[string]Volume, bool)`

GetPreCommitVolumesOk returns a tuple with the PreCommitVolumes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPreCommitVolumes

`func (o *Transaction) SetPreCommitVolumes(v map[string]map[string]Volume)`

SetPreCommitVolumes sets PreCommitVolumes field to given value.

### HasPreCommitVolumes

`func (o *Transaction) HasPreCommitVolumes() bool`

HasPreCommitVolumes returns a boolean if a field has been set.

### GetPostCommitVolumes

`func (o *Transaction) GetPostCommitVolumes() map[string]map[string]Volume`

GetPostCommitVolumes returns the PostCommitVolumes field if non-nil, zero value otherwise.

### GetPostCommitVolumesOk

`func (o *Transaction) GetPostCommitVolumesOk() (*map[string]map[string]Volume, bool)`

GetPostCommitVolumesOk returns a tuple with the PostCommitVolumes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostCommitVolumes

`func (o *Transaction) SetPostCommitVolumes(v map[string]map[string]Volume)`

SetPostCommitVolumes sets PostCommitVolumes field to given value.

### HasPostCommitVolumes

`func (o *Transaction) HasPostCommitVolumes() bool`

HasPostCommitVolumes returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


