# WalletsTransaction

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Timestamp** | **time.Time** |  | 
**Postings** | [**[]Posting**](Posting.md) |  | 
**Reference** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** | Metadata associated with the wallet. | [optional] 
**Txid** | **int64** |  | 
**PreCommitVolumes** | Pointer to [**map[string]map[string]WalletsVolume**](map.md) |  | [optional] 
**PostCommitVolumes** | Pointer to [**map[string]map[string]WalletsVolume**](map.md) |  | [optional] 

## Methods

### NewWalletsTransaction

`func NewWalletsTransaction(timestamp time.Time, postings []Posting, txid int64, ) *WalletsTransaction`

NewWalletsTransaction instantiates a new WalletsTransaction object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewWalletsTransactionWithDefaults

`func NewWalletsTransactionWithDefaults() *WalletsTransaction`

NewWalletsTransactionWithDefaults instantiates a new WalletsTransaction object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTimestamp

`func (o *WalletsTransaction) GetTimestamp() time.Time`

GetTimestamp returns the Timestamp field if non-nil, zero value otherwise.

### GetTimestampOk

`func (o *WalletsTransaction) GetTimestampOk() (*time.Time, bool)`

GetTimestampOk returns a tuple with the Timestamp field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTimestamp

`func (o *WalletsTransaction) SetTimestamp(v time.Time)`

SetTimestamp sets Timestamp field to given value.


### GetPostings

`func (o *WalletsTransaction) GetPostings() []Posting`

GetPostings returns the Postings field if non-nil, zero value otherwise.

### GetPostingsOk

`func (o *WalletsTransaction) GetPostingsOk() (*[]Posting, bool)`

GetPostingsOk returns a tuple with the Postings field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostings

`func (o *WalletsTransaction) SetPostings(v []Posting)`

SetPostings sets Postings field to given value.


### GetReference

`func (o *WalletsTransaction) GetReference() string`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *WalletsTransaction) GetReferenceOk() (*string, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *WalletsTransaction) SetReference(v string)`

SetReference sets Reference field to given value.

### HasReference

`func (o *WalletsTransaction) HasReference() bool`

HasReference returns a boolean if a field has been set.

### GetMetadata

`func (o *WalletsTransaction) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *WalletsTransaction) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *WalletsTransaction) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *WalletsTransaction) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetTxid

`func (o *WalletsTransaction) GetTxid() int64`

GetTxid returns the Txid field if non-nil, zero value otherwise.

### GetTxidOk

`func (o *WalletsTransaction) GetTxidOk() (*int64, bool)`

GetTxidOk returns a tuple with the Txid field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTxid

`func (o *WalletsTransaction) SetTxid(v int64)`

SetTxid sets Txid field to given value.


### GetPreCommitVolumes

`func (o *WalletsTransaction) GetPreCommitVolumes() map[string]map[string]WalletsVolume`

GetPreCommitVolumes returns the PreCommitVolumes field if non-nil, zero value otherwise.

### GetPreCommitVolumesOk

`func (o *WalletsTransaction) GetPreCommitVolumesOk() (*map[string]map[string]WalletsVolume, bool)`

GetPreCommitVolumesOk returns a tuple with the PreCommitVolumes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPreCommitVolumes

`func (o *WalletsTransaction) SetPreCommitVolumes(v map[string]map[string]WalletsVolume)`

SetPreCommitVolumes sets PreCommitVolumes field to given value.

### HasPreCommitVolumes

`func (o *WalletsTransaction) HasPreCommitVolumes() bool`

HasPreCommitVolumes returns a boolean if a field has been set.

### GetPostCommitVolumes

`func (o *WalletsTransaction) GetPostCommitVolumes() map[string]map[string]WalletsVolume`

GetPostCommitVolumes returns the PostCommitVolumes field if non-nil, zero value otherwise.

### GetPostCommitVolumesOk

`func (o *WalletsTransaction) GetPostCommitVolumesOk() (*map[string]map[string]WalletsVolume, bool)`

GetPostCommitVolumesOk returns a tuple with the PostCommitVolumes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPostCommitVolumes

`func (o *WalletsTransaction) SetPostCommitVolumes(v map[string]map[string]WalletsVolume)`

SetPostCommitVolumes sets PostCommitVolumes field to given value.

### HasPostCommitVolumes

`func (o *WalletsTransaction) HasPostCommitVolumes() bool`

HasPostCommitVolumes returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


