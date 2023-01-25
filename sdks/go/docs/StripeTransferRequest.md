# StripeTransferRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Amount** | Pointer to **int64** |  | [optional] 
**Asset** | Pointer to **string** |  | [optional] 
**Destination** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** | A set of key/value pairs that you can attach to a transfer object. It can be useful for storing additional information about the transfer in a structured format.  | [optional] 

## Methods

### NewStripeTransferRequest

`func NewStripeTransferRequest() *StripeTransferRequest`

NewStripeTransferRequest instantiates a new StripeTransferRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStripeTransferRequestWithDefaults

`func NewStripeTransferRequestWithDefaults() *StripeTransferRequest`

NewStripeTransferRequestWithDefaults instantiates a new StripeTransferRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAmount

`func (o *StripeTransferRequest) GetAmount() int64`

GetAmount returns the Amount field if non-nil, zero value otherwise.

### GetAmountOk

`func (o *StripeTransferRequest) GetAmountOk() (*int64, bool)`

GetAmountOk returns a tuple with the Amount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAmount

`func (o *StripeTransferRequest) SetAmount(v int64)`

SetAmount sets Amount field to given value.

### HasAmount

`func (o *StripeTransferRequest) HasAmount() bool`

HasAmount returns a boolean if a field has been set.

### GetAsset

`func (o *StripeTransferRequest) GetAsset() string`

GetAsset returns the Asset field if non-nil, zero value otherwise.

### GetAssetOk

`func (o *StripeTransferRequest) GetAssetOk() (*string, bool)`

GetAssetOk returns a tuple with the Asset field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAsset

`func (o *StripeTransferRequest) SetAsset(v string)`

SetAsset sets Asset field to given value.

### HasAsset

`func (o *StripeTransferRequest) HasAsset() bool`

HasAsset returns a boolean if a field has been set.

### GetDestination

`func (o *StripeTransferRequest) GetDestination() string`

GetDestination returns the Destination field if non-nil, zero value otherwise.

### GetDestinationOk

`func (o *StripeTransferRequest) GetDestinationOk() (*string, bool)`

GetDestinationOk returns a tuple with the Destination field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestination

`func (o *StripeTransferRequest) SetDestination(v string)`

SetDestination sets Destination field to given value.

### HasDestination

`func (o *StripeTransferRequest) HasDestination() bool`

HasDestination returns a boolean if a field has been set.

### GetMetadata

`func (o *StripeTransferRequest) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *StripeTransferRequest) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *StripeTransferRequest) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *StripeTransferRequest) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


