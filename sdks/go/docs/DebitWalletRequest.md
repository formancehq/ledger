# DebitWalletRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Amount** | [**Monetary**](Monetary.md) |  | 
**Pending** | Pointer to **bool** | Set to true to create a pending hold. If false, the wallet will be debited immediately. | [optional] 
**Metadata** | Pointer to **map[string]interface{}** | Metadata associated with the wallet. | [optional] 
**Description** | Pointer to **string** |  | [optional] 
**Destination** | Pointer to [**Subject**](Subject.md) |  | [optional] 
**Balances** | Pointer to **[]string** |  | [optional] 

## Methods

### NewDebitWalletRequest

`func NewDebitWalletRequest(amount Monetary, ) *DebitWalletRequest`

NewDebitWalletRequest instantiates a new DebitWalletRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDebitWalletRequestWithDefaults

`func NewDebitWalletRequestWithDefaults() *DebitWalletRequest`

NewDebitWalletRequestWithDefaults instantiates a new DebitWalletRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAmount

`func (o *DebitWalletRequest) GetAmount() Monetary`

GetAmount returns the Amount field if non-nil, zero value otherwise.

### GetAmountOk

`func (o *DebitWalletRequest) GetAmountOk() (*Monetary, bool)`

GetAmountOk returns a tuple with the Amount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAmount

`func (o *DebitWalletRequest) SetAmount(v Monetary)`

SetAmount sets Amount field to given value.


### GetPending

`func (o *DebitWalletRequest) GetPending() bool`

GetPending returns the Pending field if non-nil, zero value otherwise.

### GetPendingOk

`func (o *DebitWalletRequest) GetPendingOk() (*bool, bool)`

GetPendingOk returns a tuple with the Pending field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPending

`func (o *DebitWalletRequest) SetPending(v bool)`

SetPending sets Pending field to given value.

### HasPending

`func (o *DebitWalletRequest) HasPending() bool`

HasPending returns a boolean if a field has been set.

### GetMetadata

`func (o *DebitWalletRequest) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *DebitWalletRequest) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *DebitWalletRequest) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *DebitWalletRequest) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetDescription

`func (o *DebitWalletRequest) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *DebitWalletRequest) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *DebitWalletRequest) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *DebitWalletRequest) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetDestination

`func (o *DebitWalletRequest) GetDestination() Subject`

GetDestination returns the Destination field if non-nil, zero value otherwise.

### GetDestinationOk

`func (o *DebitWalletRequest) GetDestinationOk() (*Subject, bool)`

GetDestinationOk returns a tuple with the Destination field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestination

`func (o *DebitWalletRequest) SetDestination(v Subject)`

SetDestination sets Destination field to given value.

### HasDestination

`func (o *DebitWalletRequest) HasDestination() bool`

HasDestination returns a boolean if a field has been set.

### GetBalances

`func (o *DebitWalletRequest) GetBalances() []string`

GetBalances returns the Balances field if non-nil, zero value otherwise.

### GetBalancesOk

`func (o *DebitWalletRequest) GetBalancesOk() (*[]string, bool)`

GetBalancesOk returns a tuple with the Balances field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBalances

`func (o *DebitWalletRequest) SetBalances(v []string)`

SetBalances sets Balances field to given value.

### HasBalances

`func (o *DebitWalletRequest) HasBalances() bool`

HasBalances returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


