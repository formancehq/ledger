# ExpandedDebitHold

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** | The unique ID of the hold. | 
**WalletID** | **string** | The ID of the wallet the hold is associated with. | 
**Metadata** | **map[string]interface{}** | Metadata associated with the hold. | 
**Description** | **string** |  | 
**Destination** | Pointer to [**Subject**](Subject.md) |  | [optional] 
**Remaining** | **int64** | Remaining amount on hold | 
**OriginalAmount** | **int64** | Original amount on hold | 

## Methods

### NewExpandedDebitHold

`func NewExpandedDebitHold(id string, walletID string, metadata map[string]interface{}, description string, remaining int64, originalAmount int64, ) *ExpandedDebitHold`

NewExpandedDebitHold instantiates a new ExpandedDebitHold object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewExpandedDebitHoldWithDefaults

`func NewExpandedDebitHoldWithDefaults() *ExpandedDebitHold`

NewExpandedDebitHoldWithDefaults instantiates a new ExpandedDebitHold object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *ExpandedDebitHold) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *ExpandedDebitHold) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *ExpandedDebitHold) SetId(v string)`

SetId sets Id field to given value.


### GetWalletID

`func (o *ExpandedDebitHold) GetWalletID() string`

GetWalletID returns the WalletID field if non-nil, zero value otherwise.

### GetWalletIDOk

`func (o *ExpandedDebitHold) GetWalletIDOk() (*string, bool)`

GetWalletIDOk returns a tuple with the WalletID field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetWalletID

`func (o *ExpandedDebitHold) SetWalletID(v string)`

SetWalletID sets WalletID field to given value.


### GetMetadata

`func (o *ExpandedDebitHold) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *ExpandedDebitHold) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *ExpandedDebitHold) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.


### GetDescription

`func (o *ExpandedDebitHold) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *ExpandedDebitHold) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *ExpandedDebitHold) SetDescription(v string)`

SetDescription sets Description field to given value.


### GetDestination

`func (o *ExpandedDebitHold) GetDestination() Subject`

GetDestination returns the Destination field if non-nil, zero value otherwise.

### GetDestinationOk

`func (o *ExpandedDebitHold) GetDestinationOk() (*Subject, bool)`

GetDestinationOk returns a tuple with the Destination field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestination

`func (o *ExpandedDebitHold) SetDestination(v Subject)`

SetDestination sets Destination field to given value.

### HasDestination

`func (o *ExpandedDebitHold) HasDestination() bool`

HasDestination returns a boolean if a field has been set.

### GetRemaining

`func (o *ExpandedDebitHold) GetRemaining() int64`

GetRemaining returns the Remaining field if non-nil, zero value otherwise.

### GetRemainingOk

`func (o *ExpandedDebitHold) GetRemainingOk() (*int64, bool)`

GetRemainingOk returns a tuple with the Remaining field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRemaining

`func (o *ExpandedDebitHold) SetRemaining(v int64)`

SetRemaining sets Remaining field to given value.


### GetOriginalAmount

`func (o *ExpandedDebitHold) GetOriginalAmount() int64`

GetOriginalAmount returns the OriginalAmount field if non-nil, zero value otherwise.

### GetOriginalAmountOk

`func (o *ExpandedDebitHold) GetOriginalAmountOk() (*int64, bool)`

GetOriginalAmountOk returns a tuple with the OriginalAmount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOriginalAmount

`func (o *ExpandedDebitHold) SetOriginalAmount(v int64)`

SetOriginalAmount sets OriginalAmount field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


