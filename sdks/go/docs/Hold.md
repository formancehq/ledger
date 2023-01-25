# Hold

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** | The unique ID of the hold. | 
**WalletID** | **string** | The ID of the wallet the hold is associated with. | 
**Metadata** | **map[string]interface{}** | Metadata associated with the hold. | 
**Description** | **string** |  | 
**Destination** | Pointer to [**Subject**](Subject.md) |  | [optional] 

## Methods

### NewHold

`func NewHold(id string, walletID string, metadata map[string]interface{}, description string, ) *Hold`

NewHold instantiates a new Hold object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewHoldWithDefaults

`func NewHoldWithDefaults() *Hold`

NewHoldWithDefaults instantiates a new Hold object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *Hold) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Hold) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Hold) SetId(v string)`

SetId sets Id field to given value.


### GetWalletID

`func (o *Hold) GetWalletID() string`

GetWalletID returns the WalletID field if non-nil, zero value otherwise.

### GetWalletIDOk

`func (o *Hold) GetWalletIDOk() (*string, bool)`

GetWalletIDOk returns a tuple with the WalletID field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetWalletID

`func (o *Hold) SetWalletID(v string)`

SetWalletID sets WalletID field to given value.


### GetMetadata

`func (o *Hold) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Hold) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Hold) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.


### GetDescription

`func (o *Hold) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *Hold) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *Hold) SetDescription(v string)`

SetDescription sets Description field to given value.


### GetDestination

`func (o *Hold) GetDestination() Subject`

GetDestination returns the Destination field if non-nil, zero value otherwise.

### GetDestinationOk

`func (o *Hold) GetDestinationOk() (*Subject, bool)`

GetDestinationOk returns a tuple with the Destination field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestination

`func (o *Hold) SetDestination(v Subject)`

SetDestination sets Destination field to given value.

### HasDestination

`func (o *Hold) HasDestination() bool`

HasDestination returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


